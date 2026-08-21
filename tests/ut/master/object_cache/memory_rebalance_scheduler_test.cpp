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

#include <cstdint>
#include <algorithm>
#include <initializer_list>
#include <string>
#include <unordered_map>
#include <utility>

#include <gtest/gtest.h>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/timer.h"
#include "ut/common.h"

DS_DECLARE_bool(enable_memory_rebalance);
DS_DECLARE_uint32(rebalance_source_usage_percent);
DS_DECLARE_uint32(rebalance_usage_gap_percent);
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
constexpr size_t TOPOLOGY_MEMBER_ID_SIZE = 16;
constexpr size_t TOPOLOGY_DIGEST_SIZE = 64;
const std::string WORKER_92 = "127.0.0.1:9200";
const std::string WORKER_78 = "127.0.0.1:7800";
const std::string WORKER_15 = "127.0.0.1:1500";
const std::string WORKER_10 = "127.0.0.1:1000";

NodeInfo MakeNode(const std::string &worker, uint64_t usedMemory, uint64_t availableMemory, bool isReady = true,
                  uint64_t memoryCapacity = MEMORY_CAPACITY, uint64_t memoryLimit = MEMORY_CAPACITY)
{
    return NodeInfo(worker, availableMemory, isReady, 0, usedMemory, memoryCapacity, memoryLimit);
}

// Rebalance watermark in bytes == source trigger threshold (the flag is dual-role:
// it gates source selection AND caps the target migration ceiling), so tests derive
// both source-at-trigger fixtures and expected maxBytes from it instead of hardcoding.
uint64_t WatermarkBytes(uint64_t memoryLimit = MEMORY_CAPACITY)
{
    return memoryLimit * FLAGS_rebalance_source_usage_percent / 100;
}

uint64_t GapThresholdBytes(uint64_t memoryLimit = MEMORY_CAPACITY)
{
    return memoryLimit * FLAGS_rebalance_usage_gap_percent / 100;
}

// Mirrors the scheduler's anon-namespace SubOrZero so expected-value math stays underflow-safe.
uint64_t SubOrZero(uint64_t lhs, uint64_t rhs)
{
    return lhs > rhs ? lhs - rhs : 0;
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
                                                 master::RebalanceTaskStatusPb status,
                                                 master::RebalanceFailureSidePb failureSide =
                                                     master::REBALANCE_FAILURE_UNKNOWN)
{
    master::ReportRebalanceResultReqPb req;
    req.set_task_id(task.task_id());
    req.set_source_worker(task.source_worker());
    req.set_target_worker(task.target_worker());
    req.set_status(status);
    req.set_migrated_bytes(task.max_bytes());
    req.set_migrated_objects(1);
    req.set_failure_side(failureSide);
    // No fresh per-batch target memory in the legacy success/failure path: use the sentinel so
    // ReportResult does not treat 0 as a "target full" fresh signal and keeps the #685 held charge
    // until the target's own report (the legacy behavior these tests assert).
    req.set_target_remain_bytes(UINT64_MAX);
    return req;
}

// Like MakeResultReq but also carries the fresh per-batch target_remain_bytes the source worker
// forwards (the target's post-receive headroom) and the bytes actually migrated this batch. These
// drive master's next-batch decision in ReportResult (fixed-budget tracking + target safety).
master::ReportRebalanceResultReqPb MakeFreshResultReq(const master::RebalanceTaskPb &task,
                                                      uint64_t targetRemainBytes, uint64_t migratedBytes = 0)
{
    master::ReportRebalanceResultReqPb req;
    req.set_task_id(task.task_id());
    req.set_source_worker(task.source_worker());
    req.set_target_worker(task.target_worker());
    req.set_status(master::REBALANCE_TASK_SUCCEEDED);
    req.set_migrated_bytes(migratedBytes == 0 ? task.max_bytes() : migratedBytes);
    req.set_migrated_objects(1);
    req.set_target_remain_bytes(targetRemainBytes);
    return req;
}

void PublishScaleInTopology(cluster::TopologySnapshotState &snapshots,
                            std::initializer_list<std::string> workers, const std::string &leavingWorker)
{
    cluster::TopologyState topology;
    topology.clusterHasInit = true;
    topology.version = 1;
    topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_IN, 1 };
    char id = 'a';
    uint32_t token = 0;
    for (const auto &worker : workers) {
        const auto state =
            worker == leavingWorker ? cluster::MemberState::LEAVING : cluster::MemberState::ACTIVE;
        topology.members.emplace_back(
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, id++), worker }, state, { token++ } });
    }
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(cluster::TopologySnapshot::Create(
        std::move(topology), 1, std::string(TOPOLOGY_DIGEST_SIZE, 'a'), snapshot));
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(std::move(snapshot), outcome));
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
        oldTaskTimeoutS_ = FLAGS_rebalance_task_report_grace_ms;
        oldDataMigrateRate_ = FLAGS_data_migrate_rate_limit_mb;
        oldNodeDeadTimeoutS_ = FLAGS_node_dead_timeout_s;

        FLAGS_enable_memory_rebalance = true;
        FLAGS_rebalance_source_usage_percent = 80;
        FLAGS_rebalance_usage_gap_percent = 30;
        FLAGS_rebalance_task_report_grace_ms = 300;
        FLAGS_data_migrate_rate_limit_mb = 500;
        FLAGS_node_dead_timeout_s = 0;  // TTL = max(0, HOLD_TTL_MIN_S) = 60s; flaky-safe with relative backdate
    }

    void TearDown() override
    {
        FLAGS_enable_memory_rebalance = oldEnableMemoryRebalance_;
        FLAGS_rebalance_source_usage_percent = oldSourceUsagePercent_;
        FLAGS_rebalance_usage_gap_percent = oldUsageGapPercent_;
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
        for (const auto &p : s.futureView_) {
            if (p.second.inflightBytes > 0) {
                return true;
            }
        }
        return false;
    }
    static bool HasPendingRelease(const MemoryRebalanceScheduler &s)
    {
        for (const auto &p : s.futureView_) {
            if (p.second.heldBytes > 0) {
                return true;
            }
        }
        return false;
    }
    static bool HasHold(const MemoryRebalanceScheduler &s)
    {
        for (const auto &p : s.futureView_) {
            if (p.second.holdSinceMs > 0) {
                return true;
            }
        }
        return false;
    }
    static void BackdateHold(MemoryRebalanceScheduler &s, const std::string &worker, uint64_t ts)
    {
        s.futureView_[worker].holdSinceMs = ts;
    }
    static uint64_t GetHoldTs(const MemoryRebalanceScheduler &s, const std::string &worker)
    {
        return s.futureView_.at(worker).holdSinceMs;
    }
    static uint64_t GetFreshUsedMemory(const MemoryRebalanceScheduler &s, const std::string &worker)
    {
        return s.futureView_.at(worker).freshUsedMemory;
    }
    static bool HasCooldown(const MemoryRebalanceScheduler &s, const std::string &worker)
    {
        const auto iter = s.cooldownUntilMs_.find(worker);
        return iter != s.cooldownUntilMs_.end()
               && iter->second > static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    }
    static bool HasPairCooldown(const MemoryRebalanceScheduler &s, const std::string &source,
                                const std::string &target)
    {
        const auto sourceIt = s.pairCooldownUntilMs_.find(source);
        if (sourceIt == s.pairCooldownUntilMs_.end()) {
            return false;
        }
        const auto targetIt = sourceIt->second.find(target);
        return targetIt != sourceIt->second.end()
               && targetIt->second > static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    }
    static void ExpireActiveTask(MemoryRebalanceScheduler &s, const std::string &source)
    {
        s.activeTasksBySource_.at(source).task.set_deadline_ms(0);
    }
    // Backdate the chain's epoch start so BuildNextBatchTaskLocked observes an expired wall-clock
    // budget without the test having to wait 30s. Mirrors BackdateHold for the held GC TTL.
    static void BackdateEpochStart(MemoryRebalanceScheduler &s, const std::string &source, uint64_t ts)
    {
        s.activeTasksBySource_.at(source).epochStartMs = ts;
    }

private:
    bool oldEnableMemoryRebalance_ = false;
    uint32_t oldSourceUsagePercent_ = 0;
    uint32_t oldUsageGapPercent_ = 0;
    uint32_t oldTaskTimeoutS_ = 0;
    uint32_t oldDataMigrateRate_ = 0;
    uint32_t oldNodeDeadTimeoutS_ = 0;
};

// Contract test: the fixture's SetUp overrides FLAGS_rebalance_source_usage_percent to 80, so the
// formula-driven TEST_F suite still passes even if someone reverts the DS_DEFINE default back to 70.
// This standalone TEST (no fixture SetUp) reads the compiled-in default directly. The test main does
// not parse CLI flags, so the runtime value equals the DS_DEFINE default at binary start. If the
// default is reverted to 70, this test fails in CI.
TEST(RebalanceFlagDefaultTest, DefaultsTo80)
{
    EXPECT_EQ(FLAGS_rebalance_source_usage_percent, 80u);
}

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

TEST_F(MemoryRebalanceSchedulerTest, DoesNotPickLeavingWorkerAsTarget)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    PublishScaleInTopology(snapshots, { WORKER_92, WORKER_15, WORKER_10 }, WORKER_10);
    MemoryRebalanceScheduler scheduler;
    scheduler.SetTopologyMembership(&membership);
    auto resourceSnapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_15, 150, 850),
        MakeNode(WORKER_10, 100, 900),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_92, resourceSnapshot);

    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().target_worker(), WORKER_15);
}

TEST_F(MemoryRebalanceSchedulerTest, DoesNotDispatchFromLeavingSource)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    PublishScaleInTopology(snapshots, { WORKER_92, WORKER_10 }, WORKER_92);
    MemoryRebalanceScheduler scheduler;
    scheduler.SetTopologyMembership(&membership);
    auto resourceSnapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_92, resourceSnapshot);

    EXPECT_TRUE(rsp.rebalance_task().task_id().empty());
}

TEST_F(MemoryRebalanceSchedulerTest, FallsBackToResourceSnapshotBeforeTopologyIsReady)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    MemoryRebalanceScheduler scheduler;
    scheduler.SetTopologyMembership(&membership);
    auto resourceSnapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_92, resourceSnapshot);

    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().target_worker(), WORKER_10);
}

TEST_F(MemoryRebalanceSchedulerTest, UsageGapThresholdControlsWhetherTaskIsCreated)
{
    // Source sits exactly at the rebalance trigger; the gap boundary is threshold - gapPercent.
    const uint64_t sourceUsed = WatermarkBytes();
    const uint64_t boundary = SubOrZero(sourceUsed, GapThresholdBytes());
    // The verify calls below subtract a 10-byte step from `boundary`. Guard the underflow path
    // explicitly: when sourceUsed <= gap (a legal 1..100 input), boundary clamps to 0 and the
    // subtraction would wrap to ~UINT64_MAX. ASSERT_GE makes the threshold assumption explicit
    // (this test only exercises the source > gap regime) instead of silently producing a huge
    // targetUsed that happens to still yield a "task exists" result.
    ASSERT_GE(boundary, 10u);
    auto verify = [this, sourceUsed](uint64_t targetUsed, bool expectTask) {
        // Guard MEMORY_CAPACITY - targetUsed underflow: a targetUsed above capacity is a
        // physically impossible node; ASSERT_LE surfaces it instead of wrapping the subtraction.
        ASSERT_LE(targetUsed, MEMORY_CAPACITY);
        MemoryRebalanceScheduler scheduler;
        const std::string source = "127.0.0.1:7000";
        const std::string target = "127.0.0.1:" + std::to_string(targetUsed);
        auto snapshot = MakeSnapshot({
            MakeNode(source, sourceUsed, MEMORY_CAPACITY - sourceUsed),
            MakeNode(target, targetUsed, MEMORY_CAPACITY - targetUsed),
        });

        auto rsp = ScheduleAndGetRsp(scheduler, source, snapshot);
        EXPECT_EQ(!rsp.rebalance_task().task_id().empty(), expectTask);
    };

    verify(boundary + 10, false);  // gap = threshold - 1 below boundary
    verify(boundary, true);         // gap == boundary (exactly meets)
    verify(boundary - 10, true);    // gap above boundary
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
        auto failedReq = MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_FAILED,
                                       master::REBALANCE_FAILURE_TARGET);
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
        auto failedReq = MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_FAILED,
                                       master::REBALANCE_FAILURE_TARGET);
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

// Issue #958 target-attributed failures must cooldown the TARGET, not the SOURCE. With the source
// cooldowned too, the scheduler cannot switch that healthy source to another target.
TEST_F(MemoryRebalanceSchedulerTest, FailedTaskCooldownsTargetButNotSource)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());

    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(
        scheduler.ReportResult(MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_FAILED,
                                             master::REBALANCE_FAILURE_TARGET),
                               reportRsp));

    EXPECT_TRUE(HasCooldown(scheduler, WORKER_10));
    EXPECT_FALSE(HasCooldown(scheduler, WORKER_92));
}

// Issue #958: after a failed rebalance task the TARGET's cooldown must survive the target's own
// resource reports. ExpireTimeoutTasksLocked used to erase the cooldown entry of the reporting
// worker (`iter->first == activeWorker`), so a UB-faulted target that still reports over TCP kept
// clearing its own exclusion and was re-selected every round.
TEST_F(MemoryRebalanceSchedulerTest, FailedTargetCooldownSurvivesTargetResourceReport)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_78, 150, 850),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), WORKER_10);

    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(
        scheduler.ReportResult(MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_FAILED,
                                             master::REBALANCE_FAILURE_TARGET),
                               reportRsp));
    ASSERT_TRUE(HasCooldown(scheduler, WORKER_10));

    // The failed target reports its own resource usage. The cooldown must NOT be cleared by the
    // report itself (the erase clause `iter->first == activeWorker` is removed).
    master::ResourceReportRspPb targetRsp;
    DS_ASSERT_OK(scheduler.Schedule(MakeResourceReq(WORKER_10), snapshot, targetRsp));
    EXPECT_TRUE(HasCooldown(scheduler, WORKER_10));

    // While the failed target is cooldowned, the next round from the source picks another target.
    auto secondRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_NE(secondRsp.rebalance_task().target_worker(), WORKER_10);
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), WORKER_78);
}

TEST_F(MemoryRebalanceSchedulerTest, ExpiredTaskCooldownsPairBeforeImmediateReschedule)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_78, 150, 850),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), WORKER_10);

    ExpireActiveTask(scheduler, WORKER_92);
    auto secondRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);

    EXPECT_TRUE(HasPairCooldown(scheduler, WORKER_92, WORKER_10));
    EXPECT_FALSE(HasCooldown(scheduler, WORKER_92));
    EXPECT_FALSE(HasCooldown(scheduler, WORKER_10));
    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), WORKER_78);
}

TEST_F(MemoryRebalanceSchedulerTest, FailureAttributionControlsCooldownScope)
{
    const auto verifyNodeCooldown = [this](master::RebalanceFailureSidePb side, const std::string &expectedWorker) {
        MemoryRebalanceScheduler scheduler;
        auto snapshot = MakeSnapshot({ MakeNode(WORKER_92, 920, 80), MakeNode(WORKER_10, 100, 900) });
        auto rsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
        master::ReportRebalanceResultRspPb reportRsp;
        DS_ASSERT_OK(scheduler.ReportResult(MakeResultReq(rsp.rebalance_task(), master::REBALANCE_TASK_FAILED, side),
                                            reportRsp));
        EXPECT_EQ(HasCooldown(scheduler, WORKER_92), expectedWorker == WORKER_92);
        EXPECT_EQ(HasCooldown(scheduler, WORKER_10), expectedWorker == WORKER_10);
    };
    verifyNodeCooldown(master::REBALANCE_FAILURE_SOURCE, WORKER_92);
    verifyNodeCooldown(master::REBALANCE_FAILURE_TARGET, WORKER_10);
    verifyNodeCooldown(master::REBALANCE_FAILURE_NO_CANDIDATE, WORKER_92);
    verifyNodeCooldown(master::REBALANCE_FAILURE_CONTROL_PLANE, "");

    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({ MakeNode(WORKER_92, 920, 80), MakeNode(WORKER_78, 150, 850),
                                   MakeNode(WORKER_10, 100, 900) });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(scheduler.ReportResult(MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_FAILED),
                                        reportRsp));
    EXPECT_TRUE(HasPairCooldown(scheduler, WORKER_92, WORKER_10));
    EXPECT_FALSE(HasCooldown(scheduler, WORKER_92));
    EXPECT_FALSE(HasCooldown(scheduler, WORKER_10));
    auto secondRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), WORKER_78);
}

TEST_F(MemoryRebalanceSchedulerTest, HeldInflightReducesBudgetForImmediateRepick)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());

    // Success holds the in-flight charge (issue #685): the target's snapshot is still
    // stale-low, so the held in-flight reduces the re-pick budget via the projected-usage
    // watermark clamp. projectedUsed = 100 + 410 = 510; headroom = watermark - 510;
    // targetAvail = 900 - 410 = 490. maxBytes = min(410, headroom, 490) -- watermark binds.
    master::ReportRebalanceResultRspPb reportRsp;
    auto successReq = MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED);
    DS_ASSERT_OK(scheduler.ReportResult(successReq, reportRsp));

    auto secondRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), WORKER_10);
    EXPECT_EQ(secondRsp.rebalance_task().max_bytes(),
              std::min({ 410ul, SubOrZero(WatermarkBytes(), 510), SubOrZero(900u, 410u) }));
}

// ============================================================================
// Issue #685: Memory Rebalance target over-receive + oscillation.
// https://gitcode.com/openeuler/yuanrong-datasystem/issues/685
//
// Root cause (verified against logs685 + source): three factors combined to
// over-fill the target:
//   1. maxBytes used a midpoint (source-target)/2 but the available cap allowed
//      reaching exactly the eviction trigger line (equality fires eviction);
//   2. the availableMemory used for the cap was the STALE snapshot value (worker
//      report every 30s + master merge every 10s => up to ~40s lag, so stale-low
//      usedMemory => stale-high available => budget stays > 0);
//   3. RemoveTaskLocked cleared the in-flight charge at completion BEFORE the
//      target's snapshot reflected the received bytes, so the just-received
//      target was re-pickable in the next window.
//
// Fix = available cap + held-inflight + fresh projection:
//   1. CalculateTaskBytesLocked caps maxBytes at availableMemory - inflight so
//      projected used + inflight + maxBytes does not EXCEED the eviction trigger
//      line (eviction fires at >= in the worker's Allocate path, which is the
//      eviction manager's job, not the scheduler's). The trigger line uses BOTH
//      eviction params (ratio + reserve) via availableMemory.
//   2. ReportResource merges snapshots before Schedule (merge-on-trigger) so
//      projection uses fresh post-receive memory, not stale-low.
//   3. Success holds the in-flight charge (heldBytes) until the target reports
//      its real post-receive memory (ReleaseReporterHoldsLocked was removed from
//      the target's own report; ReleaseSnapshotHoldsLocked as a merge-lagged backup
//      now carries the release). This prevents sequential re-pick of the
//      just-received target.
//   4. migrated_bytes uses sallocx real size (#1346) so projection is accurate.
//
// Why no infinite oscillation: single migration moves (source-target)/2 to the
// midpoint, not the full gap. The only over-concentration is the concurrent case
// (two sources, T stale-low): T can reach ~triggerLine for ONE cycle, then
// becomes a source and redistributes (converges to cluster average). The available
// cap ensures projected does not exceed triggerLine; held-inflight bounds concurrent sources.
// ============================================================================

// Sequential repick (H1): after S1->T1 succeeds, the in-flight charge is held. S2
// reports while T1's snapshot is still stale-low; the held in-flight reduces S2's
// budget for T1 but does NOT reject it (watermark ceiling removed). maxBytes = min(400,
// 900-400=500) = 400, projected = 900 < triggerLine(1000). S2 is dispatched to T1.
TEST_F(MemoryRebalanceSchedulerTest, HeldInflightReducesBudgetForSequentialRepickToReceivedTarget)
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
    // in-flight reduces S2's budget via the projected-usage watermark clamp:
    // projectedUsed = 100 + 400 = 500; headroom = watermark - 500; targetAvail = 900 - 400 = 500.
    // maxBytes = min(400, headroom, 500) -- watermark binds. S2 targets T1 directly.
    auto secondSnapshot = MakeSnapshot({
        MakeNode(source1, 500, 500),  // S1 drained in reality
        MakeNode(source2, 900, 100),  // S2 is the reporting source
        MakeNode(target1, 100, 900),  // STALE: still looks 10% / 900 available
        MakeNode(target2, 300, 700),
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, source2, secondSnapshot);

    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().source_worker(), source2);
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), target1);
    EXPECT_EQ(secondRsp.rebalance_task().max_bytes(),
              std::min({ 400ul, SubOrZero(WatermarkBytes(), 500), SubOrZero(900u, 400u) }));
}

// Concurrent (H3 / logs685 W2): while S1->T is still active (in-flight[T]=445),
// S2 reports. The available cap reduces S2's budget so projected = 100+445+355 =
// 900 = triggerLine(900). S2 is dispatched (not rejected); T fills up to but does
// not exceed the eviction trigger line.
TEST_F(MemoryRebalanceSchedulerTest, ConcurrentSourcesFillTargetUpToTriggerLine)
{
    const std::string source1 = "127.0.0.1:9302";
    const std::string source2 = "127.0.0.1:8302";
    const std::string target = "127.0.0.1:1302";
    MemoryRebalanceScheduler scheduler;

    // triggerLine = 100 + 800 = 900 (tighter than 1000 to make the clamp, not midpoint, bind).
    auto snap = MakeSnapshot({
        MakeNode(source1, 990, 10),
        MakeNode(source2, 850, 150),
        MakeNode(target, 100, 800),
    });
    // S1 (99%) -> T (10%): maxBytes = min(445, 800-0=800) = 445, in-flight[T]=445.
    auto rsp1 = ScheduleAndGetRsp(scheduler, source1, snap);
    ASSERT_FALSE(rsp1.rebalance_task().task_id().empty());
    ASSERT_EQ(rsp1.rebalance_task().target_worker(), target);
    ASSERT_EQ(rsp1.rebalance_task().max_bytes(), 445ul);

    // S2 (85%) reports while S1 is active. The watermark clamp now accounts for S1's
    // in-flight: projectedUsed = 100 + 445 = 545; headroom = watermark - 545;
    // targetAvail = 800 - 445 = 355. maxBytes = min(375, headroom, 355) -- watermark binds.
    auto rsp2 = ScheduleAndGetRsp(scheduler, source2, snap);
    ASSERT_FALSE(rsp2.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp2.rebalance_task().target_worker(), target);
    EXPECT_EQ(rsp2.rebalance_task().max_bytes(),
              std::min({ 375ul, SubOrZero(WatermarkBytes(), 545), SubOrZero(800u, 445u) }));
}

// Baseline (passes now): when the snapshot is kept fresh (each schedule reflects the
// actual post-migration usage), no re-pick or oscillation occurs -- the source drops
// below the source threshold and no second task is built. Proves staleness (not the
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

    // Fresh snapshot: source actually drained to 50% (< source threshold) => not a
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
    // TTL GC released the held charge: heldBytes + holdSinceMs are gone. We do NOT assert
    // futureView_ empty here -- the second dispatch (below) just added the new task's charge
    // back into it. The GC itself is proven by heldBytes/holdSinceMs being zero and by the
    // second task being assigned (projected 51%, well below triggerLine).
    EXPECT_FALSE(HasPendingRelease(scheduler));
    EXPECT_FALSE(HasHold(scheduler));
    // After GC the target's in-flight is 0, so it is selectable again (projected 51%, well below triggerLine).
    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), WORKER_10);
}

// M2 (issue #685): ReleaseSnapshotHoldsLocked is the merge-lagged backup release path. When a
// non-reporting target's snapshot timestamp advances past its held completion time (the master
// merged in a newer snapshot), the held charge is released even though that target did not just
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
    // Snapshot-based release cleared the held charge: heldBytes + holdSinceMs are gone.
    // (futureView_ is not empty -- the second dispatch just added its charge back; the
    // release itself is proven by heldBytes/holdSinceMs being zero and by the second task
    // being assigned, projected 51% well below triggerLine.)
    EXPECT_FALSE(HasPendingRelease(scheduler));
    EXPECT_FALSE(HasHold(scheduler));
    // After release the target is selectable again (projected 51%, well below triggerLine).
    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), WORKER_10);
}

// M3 (issue #685): the target reports its own resource via NeedSnapshotForSchedule, but with
// ReleaseReporterHoldsLocked removed, the held charge is NOT released here. It stays until a
// source's Schedule calls ReleaseSnapshotHoldsLocked with a fresh snapshot. This prevents the
// #685 window where the held was released before the snapshot was refreshed.
TEST_F(MemoryRebalanceSchedulerTest, ReporterReportDoesNotReleaseHeldUntilSnapshotFresh)
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

    // WORKER_10 reports itself via NeedSnapshotForSchedule. With ReleaseReporterHoldsLocked removed,
    // the held charge is NOT released here — it must stay until the snapshot is refreshed.
    const uint64_t holdTs = GetHoldTs(scheduler, WORKER_10);
    NodeInfo targetReport(WORKER_10, 900, true, holdTs + 1, 100, MEMORY_CAPACITY, MEMORY_CAPACITY);
    master::ResourceReportRspPb rsp;
    auto req = MakeResourceReq(WORKER_10);
    (void)scheduler.NeedSnapshotForSchedule(req, targetReport, rsp);
    EXPECT_TRUE(HasPendingRelease(scheduler));
    EXPECT_TRUE(HasHold(scheduler));

    // Now a source reports via Schedule with a snapshot where WORKER_10's timestamp advanced past
    // holdTs. ReleaseSnapshotHoldsLocked (called from Schedule) releases the held charge.
    NodeInfo freshTarget(WORKER_10, 900, true, holdTs + 1, 100, MEMORY_CAPACITY, MEMORY_CAPACITY);
    auto freshSnapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        freshTarget,
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, WORKER_92, freshSnapshot);
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

// Verifies the available cap in CalculateTaskBytesLocked: the target's post-migration
// projected usage must not EXCEED the eviction trigger line (availableMemory encodes
// triggerLine - used, using BOTH eviction params). The cap binds maxBytes to the
// available headroom minus inflight, preventing over-migration past the trigger line.
TEST_F(MemoryRebalanceSchedulerTest, TargetClampBindsMaxBytesToAvailableHeadroom)
{
    const std::string source = "127.0.0.1:9305";
    const std::string target = "127.0.0.1:1305";

    // Case 1: ample headroom → midpoint binds, projected well below triggerLine.
    {
        MemoryRebalanceScheduler scheduler;
        auto snap = MakeSnapshot({
            MakeNode(source, 900, 100),   // triggerLine = 1000
            MakeNode(target, 100, 850),   // available = 850, triggerLine = 950
        });
        auto rsp = ScheduleAndGetRsp(scheduler, source, snap);
        ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
        EXPECT_EQ(rsp.rebalance_task().max_bytes(), 400ul);  // min(400, 850, ...) = 400
    }

    // Case 2: tight headroom → clamp binds (available < gap/2), verify cap boundary.
    {
        MemoryRebalanceScheduler scheduler;
        auto snap = MakeSnapshot({
            MakeNode(source, 990, 10),    // triggerLine = 1000
            MakeNode(target, 100, 100),   // available = 100, triggerLine = 200
        });
        auto rsp = ScheduleAndGetRsp(scheduler, source, snap);
        ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
        // min((990-100)/2=445, SubOrZero(100, 0)=100, ...) = 100 (clamp binds, not midpoint)
        EXPECT_EQ(rsp.rebalance_task().max_bytes(), 100ul);
        // projected = 100 + 0 + 100 = 200 = triggerLine. Does not exceed.
    }

    // Case 3: no headroom (target at triggerLine) → maxBytes=0 → skipped.
    {
        MemoryRebalanceScheduler scheduler;
        auto snap = MakeSnapshot({
            MakeNode(source, 900, 100),   // triggerLine = 1000
            MakeNode(target, 100, 0),     // available = 0, already at triggerLine
        });
        auto rsp = ScheduleAndGetRsp(scheduler, source, snap);
        EXPECT_TRUE(rsp.rebalance_task().task_id().empty());
    }
}

// ============================================================================
// Per-batch feedback loop: ReportResult builds the next 300MB batch task from
// the fixed total budget (set at Schedule) and the fresh target_remain_bytes
// the worker forwards, with dual stop conditions (budget exhausted OR target
// reached the watermark).
// ============================================================================

namespace {
constexpr uint64_t ONE_GB = 1024ull * 1024 * 1024;
constexpr uint64_t MB = 1024 * 1024;
}  // namespace

// A single 300MB batch does not exhaust a GB-scale budget, so master returns a next task whose
// max_bytes is the remaining budget (capped by 300MB / target headroom). The loop terminates when
// the fixed budget (set at Schedule) is fully migrated.
TEST_F(MemoryRebalanceSchedulerTest, ReportResultReturnsNextBatchUntilBudgetExhausted)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    // memoryCapacity == memoryLimit == highWater line == 1GB; watermark = source_pct of 1GB.
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900 * MB, ONE_GB - 900 * MB, true, ONE_GB, ONE_GB),  // 900MB used, hot
        MakeNode(target, 100 * MB, ONE_GB - 100 * MB, true, ONE_GB, ONE_GB),  // 100MB used
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    // totalBudget = min(usageGap=(900-100)/2=400MB, headroomToWatermark=watermark-100MB, targetAvail=924MB)
    // = 400MB; first batch = min(300MB, 400MB) = 300MB.
    EXPECT_EQ(firstRsp.rebalance_task().max_bytes(), 300 * MB);

    // Batch 1 report: migrated 300MB, target received 300MB -> used 400MB, remain = 1GB-400MB.
    // remaining budget = 400-300 = 100MB; target rate 39% < source threshold; next batch = 100MB (budget binds).
    master::ReportRebalanceResultRspPb batch1Rsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(firstRsp.rebalance_task(), ONE_GB - 400 * MB, 300 * MB), batch1Rsp));
    ASSERT_TRUE(batch1Rsp.has_next_rebalance_task());
    const auto &nextTask = batch1Rsp.next_rebalance_task();
    EXPECT_EQ(nextTask.source_worker(), source);
    EXPECT_EQ(nextTask.target_worker(), target);
    EXPECT_EQ(nextTask.max_bytes(), 100 * MB);  // remaining budget, not fresh gap

    // Batch 2 report: migrated the last 100MB, cumulative = 400MB = totalBudget -> stop.
    master::ReportRebalanceResultRspPb batch2Rsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(nextTask, ONE_GB - 500 * MB, 100 * MB), batch2Rsp));
    EXPECT_FALSE(batch2Rsp.has_next_rebalance_task());
}

// When the fixed total budget is migrated in one batch (byte-scale, 300MB cap does not bind),
// master must NOT assign another batch: budget exhausted.
TEST_F(MemoryRebalanceSchedulerTest, ReportResultStopsWhenBudgetExhausted)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900, 100),  // capacity=limit=1000
        MakeNode(target, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    // totalBudget = min((900-100)/2=400, headroomToWatermark=watermark-100, targetAvail=900) = 400; batch=400.
    EXPECT_EQ(firstRsp.rebalance_task().max_bytes(), 400ul);

    // Migrated the full 400 budget -> cumulative=400=totalBudget -> stop.
    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(firstRsp.rebalance_task(), /*targetRemain*/ 500, 400), reportRsp));
    EXPECT_FALSE(reportRsp.has_next_rebalance_task());
}

// The fresh target safety stop fires INDEPENDENTLY of the budget when the target's own foreground
// writes push it to the watermark before the fixed budget is exhausted.
TEST_F(MemoryRebalanceSchedulerTest, ReportResultStopsWhenTargetReachesWatermarkWithBudgetRemaining)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    // source 900MB, target 200MB; watermark = source_pct of 1GB. totalBudget = min((900-200)/2=350MB,
    // headroomToWatermark = watermark-200MB, targetAvail=824MB) = 350MB; first batch = 300MB (cap binds).
    // Target-safety crossing point is watermark + 50MB (always above the watermark regardless of the
    // configured source threshold), simulating foreground writes pushing the target over the line.
    const uint64_t crossUsed = WatermarkBytes(ONE_GB) + 50 * MB;
    // Guard ONE_GB - crossUsed underflow: when the source threshold is >= 96%, watermark + 50MB
    // exceeds 1GB and the "remain" subtraction would wrap to a huge value. ASSERT_LE surfaces this
    // as a physically-impossible-node failure instead of silently reversing the fresh-signal logic.
    ASSERT_LE(crossUsed, ONE_GB);
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900 * MB, ONE_GB - 900 * MB, true, ONE_GB, ONE_GB),
        MakeNode(target, 200 * MB, ONE_GB - 200 * MB, true, ONE_GB, ONE_GB),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(firstRsp.rebalance_task().max_bytes(), 300 * MB);

    // Batch 1: migrated 300MB (cumulative=300, budget remaining=50MB), BUT the target also had
    // foreground writes that pushed its used to crossUsed (remain = 1GB - crossUsed). fresh target
    // rate = crossUsed/1GB >= source threshold -> stop, even though 50MB of budget remains.
    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(firstRsp.rebalance_task(), ONE_GB - crossUsed, 300 * MB), reportRsp));
    EXPECT_FALSE(reportRsp.has_next_rebalance_task());
}

// When a fresh report signals the target crossed the rebalance watermark (e.g. remain=50MB ->
// usage 95% >= source threshold) and the chain ends, the held charge is KEPT (not released) so the #685
// stale-snapshot guard protects the target until its own ResourceReport arrives. This prevents
// sequential re-pick of the just-received target.
TEST_F(MemoryRebalanceSchedulerTest, ReportResultKeepsHeldWhenChainEndsForTargetSafety)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900 * MB, ONE_GB - 900 * MB, true, ONE_GB, ONE_GB),
        MakeNode(target, 100 * MB, ONE_GB - 100 * MB, true, ONE_GB, ONE_GB),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(firstRsp.rebalance_task().max_bytes(), 300 * MB);

    // Fresh report: target crossed watermark (remain=50MB -> used 950MB = 95% >= source threshold). The chain
    // ends via the watermark stop and the held charge is KEPT for #685 protection (not released).
    // No next batch assigned.
    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(firstRsp.rebalance_task(), /*targetRemain*/ 50 * MB, 300 * MB), reportRsp));
    EXPECT_TRUE(HasPendingRelease(scheduler)) << "held must be kept when chain ends for #685 protection";
    EXPECT_FALSE(reportRsp.has_next_rebalance_task());
}

// Idempotent ReportResult: if the predecessor's ReportResult response is lost in flight, the
// worker retries the same predecessor. Master sees a stale task_id (the active entry is the
// successor we already built). Without replay, master returns an empty OK and the worker breaks
// the loop prematurely. With replay, master returns the cached successor so the chain continues.
TEST_F(MemoryRebalanceSchedulerTest, ReportResultReplaysCachedSuccessorOnRetry)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900 * MB, ONE_GB - 900 * MB, true, ONE_GB, ONE_GB),
        MakeNode(target, 100 * MB, ONE_GB - 100 * MB, true, ONE_GB, ONE_GB),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    const auto &taskA = firstRsp.rebalance_task();
    EXPECT_EQ(taskA.max_bytes(), 300 * MB);

    // First report: success, fresh remain signals target has room. Master builds successor B.
    master::ReportRebalanceResultRspPb rsp1;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskA, ONE_GB - 400 * MB, 300 * MB), rsp1));
    ASSERT_TRUE(rsp1.has_next_rebalance_task());
    const auto successorTaskId = rsp1.next_rebalance_task().task_id();
    EXPECT_NE(successorTaskId, taskA.task_id());

    // Retry: worker retries the predecessor (response was lost). Master must replay the same
    // successor instead of returning an empty OK that would break the chain.
    master::ReportRebalanceResultRspPb rsp2;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskA, ONE_GB - 400 * MB, 300 * MB), rsp2));
    ASSERT_TRUE(rsp2.has_next_rebalance_task());
    EXPECT_EQ(rsp2.next_rebalance_task().task_id(), successorTaskId)
        << "retry of predecessor must replay the same cached successor";
}

// Middle-of-chain retry: the replay must fire for ANY immediate predecessor in the chain, not
// only the first task. After A->B->C, retrying B (the immediate predecessor of C) must replay
// C. This locks in the contract that immediatePredecessorTaskId is updated on every hop.
TEST_F(MemoryRebalanceSchedulerTest, ReportResultReplaysSuccessorOnMiddleOfChainRetry)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    constexpr uint64_t TWO_GB = 2ULL * ONE_GB;
    // Source stays above the trigger at any configured threshold (watermark + 300MB). The 1400MB
    // source-target gap keeps gap/2 = 700MB so the 3-hop chain (A=300, B=300, C=100) is stable
    // regardless of the source threshold.
    const uint64_t sourceUsed = WatermarkBytes(TWO_GB) + 300 * MB;
    const uint64_t targetUsed = SubOrZero(sourceUsed, 1400 * MB);
    // Guard TWO_GB - sourceUsed / targetUsed underflow: at pct=100, watermark + 300MB exceeds TWO_GB
    // and the available-memory subtraction would wrap. ASSERT_LE makes the threshold assumption
    // (pct < 100 here) explicit instead of silently producing a physically-impossible node.
    ASSERT_LE(sourceUsed, TWO_GB);
    ASSERT_LE(targetUsed, TWO_GB);
    auto snapshot = MakeSnapshot({
        MakeNode(source, sourceUsed, TWO_GB - sourceUsed, true, TWO_GB, TWO_GB),
        MakeNode(target, targetUsed, TWO_GB - targetUsed, true, TWO_GB, TWO_GB),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    const auto &taskA = firstRsp.rebalance_task();
    EXPECT_EQ(taskA.max_bytes(), 300 * MB);

    // Hop 1: report A -> B built (target used targetUsed+300MB; rate < source threshold;
    // remaining budget 700-300=400MB; nextBytes=min(300,400,...)=300MB).
    master::ReportRebalanceResultRspPb rsp1;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskA, TWO_GB - (targetUsed + 300 * MB), 300 * MB), rsp1));
    ASSERT_TRUE(rsp1.has_next_rebalance_task());
    const auto &taskB = rsp1.next_rebalance_task();
    EXPECT_EQ(taskB.max_bytes(), 300 * MB);

    // Hop 2: report B -> C built (target used targetUsed+600MB; rate < source threshold;
    // remaining budget 700-600=100MB; nextBytes=min(300,100,...)=100MB).
    // active entry is now C with immediatePredecessorTaskId = taskB.task_id().
    master::ReportRebalanceResultRspPb rsp2;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskB, TWO_GB - (targetUsed + 600 * MB), 300 * MB), rsp2));
    ASSERT_TRUE(rsp2.has_next_rebalance_task());
    const auto taskCId = rsp2.next_rebalance_task().task_id();
    EXPECT_NE(taskCId, taskB.task_id());

    // Retry B (immediate predecessor of C): must replay C, not return empty OK.
    master::ReportRebalanceResultRspPb retryRsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskB, TWO_GB - (targetUsed + 600 * MB), 300 * MB), retryRsp));
    ASSERT_TRUE(retryRsp.has_next_rebalance_task());
    EXPECT_EQ(retryRsp.next_rebalance_task().task_id(), taskCId)
        << "retry of middle-of-chain predecessor must replay the same cached successor";
}

// Negative case: retrying an OLDER predecessor (not the immediate one) must NOT replay. After
// A->B->C, retrying A finds active entry C whose immediatePredecessorTaskId = B != A. Master returns
// empty OK and the worker breaks the loop. This locks in the "only immediate predecessor"
// contract -- without it, a stale retry could re-issue an already-superseded task.
TEST_F(MemoryRebalanceSchedulerTest, ReportResultDoesNotReplayForOlderPredecessorRetry)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    constexpr uint64_t TWO_GB = 2ULL * ONE_GB;
    // Same threshold-agnostic setup as the middle-of-chain test: watermark+300MB source, 1400MB gap.
    const uint64_t sourceUsed = WatermarkBytes(TWO_GB) + 300 * MB;
    const uint64_t targetUsed = SubOrZero(sourceUsed, 1400 * MB);
    // Guard TWO_GB - sourceUsed / targetUsed underflow (same rationale as the middle-of-chain test).
    ASSERT_LE(sourceUsed, TWO_GB);
    ASSERT_LE(targetUsed, TWO_GB);
    auto snapshot = MakeSnapshot({
        MakeNode(source, sourceUsed, TWO_GB - sourceUsed, true, TWO_GB, TWO_GB),
        MakeNode(target, targetUsed, TWO_GB - targetUsed, true, TWO_GB, TWO_GB),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    const auto &taskA = firstRsp.rebalance_task();

    // Build chain A -> B -> C (same setup as the middle-of-chain test).
    master::ReportRebalanceResultRspPb rsp1;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskA, TWO_GB - (targetUsed + 300 * MB), 300 * MB), rsp1));
    ASSERT_TRUE(rsp1.has_next_rebalance_task());
    const auto &taskB = rsp1.next_rebalance_task();

    master::ReportRebalanceResultRspPb rsp2;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskB, TWO_GB - (targetUsed + 600 * MB), 300 * MB), rsp2));
    ASSERT_TRUE(rsp2.has_next_rebalance_task());

    // Retry A (older predecessor). Active entry is C with immediatePredecessorTaskId = B != A.
    // Must NOT replay; return empty OK so the worker stops the loop.
    master::ReportRebalanceResultRspPb retryRsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskA, TWO_GB - (targetUsed + 300 * MB), 300 * MB), retryRsp));
    EXPECT_FALSE(retryRsp.has_next_rebalance_task())
        << "older predecessor retry must not replay; only the immediate predecessor is replayed";
}

// State idempotency: a retry of the immediate predecessor must not mutate scheduler state
// (inflight, held, cooldowns). The replay branch only writes to the response proto. This test
// records the inflight/held booleans before and after the retry and asserts they are unchanged.
TEST_F(MemoryRebalanceSchedulerTest, ReportResultReplayIsStateIdempotent)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900 * MB, ONE_GB - 900 * MB, true, ONE_GB, ONE_GB),
        MakeNode(target, 100 * MB, ONE_GB - 100 * MB, true, ONE_GB, ONE_GB),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    const auto &taskA = firstRsp.rebalance_task();

    // Report A success -> B built. State after first report: target has inflight (B's 100MB),
    // held released to 0 (B was built, so held was released inside BuildNextBatchTaskLocked block).
    master::ReportRebalanceResultRspPb rsp1;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskA, ONE_GB - 400 * MB, 300 * MB), rsp1));
    ASSERT_TRUE(rsp1.has_next_rebalance_task());
    const auto successorTaskId = rsp1.next_rebalance_task().task_id();
    const bool inflightBefore = HasInflight(scheduler);
    const bool heldBefore = HasPendingRelease(scheduler);
    EXPECT_TRUE(inflightBefore) << "B's max_bytes must be charged as inflight after B is built";
    EXPECT_FALSE(heldBefore) << "held must be released when B is built (chain continues)";

    // Retry A: replay B from cache. State must NOT change (replay is read-only).
    master::ReportRebalanceResultRspPb rsp2;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskA, ONE_GB - 400 * MB, 300 * MB), rsp2));
    ASSERT_TRUE(rsp2.has_next_rebalance_task());
    EXPECT_EQ(rsp2.next_rebalance_task().task_id(), successorTaskId);
    EXPECT_EQ(HasInflight(scheduler), inflightBefore)
        << "retry must not change inflight state (replay is read-only)";
    EXPECT_EQ(HasPendingRelease(scheduler), heldBefore)
        << "retry must not change held state (replay is read-only)";
}

// The chain's wall-clock duration is capped to one 30s ResourceReport cycle. If the chain runs
// past the epoch budget, BuildNextBatchTaskLocked must stop the loop even if budget and target
// headroom remain. This prevents source/target duty cycle saturation on large gaps.
TEST_F(MemoryRebalanceSchedulerTest, EpochWallClockBudgetStopsChain)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900 * MB, ONE_GB - 900 * MB, true, ONE_GB, ONE_GB),
        MakeNode(target, 100 * MB, ONE_GB - 100 * MB, true, ONE_GB, ONE_GB),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    const auto &taskA = firstRsp.rebalance_task();
    EXPECT_EQ(taskA.max_bytes(), 300 * MB);

    // Backdate the chain's epoch start so BuildNextBatchTaskLocked observes an expired 30s budget.
    const uint64_t nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    BackdateEpochStart(scheduler, source, nowMs - 31 * MS_PER_SECOND);

    // Report success with fresh remain signaling room (target only at 40% usage). Without the
    // epoch check, master would build a next batch. With the check, the chain stops.
    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskA, ONE_GB - 400 * MB, 300 * MB), reportRsp));
    EXPECT_FALSE(reportRsp.has_next_rebalance_task())
        << "chain must stop when wall-clock epoch budget is exhausted";
    EXPECT_TRUE(HasPendingRelease(scheduler))
        << "held must be kept when chain ends on epoch budget stop for #685 protection";
}

// Defensive guard against an impossible "fresh remain > capacity" signal. Without this guard,
// SubOrZero below would silently reverse to freshTargetUsed=0 (target looks empty), keeping the
// loop going against a genuinely-full target. Treat as fresh-signal corrupt: stop the chain.
TEST_F(MemoryRebalanceSchedulerTest, FreshRemainAboveCapacityStopsChain)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900 * MB, ONE_GB - 900 * MB, true, ONE_GB, ONE_GB),
        MakeNode(target, 100 * MB, ONE_GB - 100 * MB, true, ONE_GB, ONE_GB),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    const auto &taskA = firstRsp.rebalance_task();

    // Fresh remain > capacity (impossible in practice but unguarded): master must refuse to
    // build a next batch instead of silently reversing to 0% usage.
    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(taskA, /*targetRemain*/ 2 * ONE_GB, 300 * MB), reportRsp));
    EXPECT_FALSE(reportRsp.has_next_rebalance_task())
        << "fresh remain > capacity must stop the chain (signal corrupt, not silent reversal to 0%)";
    EXPECT_TRUE(HasPendingRelease(scheduler))
        << "held must be kept when chain ends on capacity-guard stop for #685 protection";
}

// Multi-source: S1's chain ends with freshUsedMemory set from target_remain_bytes. S2's Schedule
// sees the real target usage via freshUsedMemory (not the stale snapshot), preventing over-migration.
TEST_F(MemoryRebalanceSchedulerTest, FreshUsedMemoryPreventsMultiSourceOverMigration)
{
    // The gap gate now uses effectiveTargetUsed (max(snapshot, fresh)). With the default 30%
    // threshold, the effective gap (90%-70%=20%) would filter the pair. Lower it so the pair
    // passes and the test can verify maxBytes capping by freshUsedMemory.
    FLAGS_rebalance_usage_gap_percent = 10;
    const std::string source1 = "127.0.0.1:9301";
    const std::string source2 = "127.0.0.1:9302";
    const std::string target = "127.0.0.1:1301";

    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(source1, 900, 100),
        MakeNode(source2, 900, 100),
        MakeNode(target, 100, 900),  // stale: shows 10%, real is 70%
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source1, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), target);

    // S1's batch succeeds. Worker reports target_remain_bytes showing real usage = 700.
    // ProcessFreshFeedbackLocked sets futureView_[target].freshUsedMemory = 700.
    master::ReportRebalanceResultReqPb resultReq;
    resultReq.set_task_id(firstRsp.rebalance_task().task_id());
    resultReq.set_source_worker(source1);
    resultReq.set_target_worker(target);
    resultReq.set_status(master::REBALANCE_TASK_SUCCEEDED);
    resultReq.set_migrated_bytes(firstRsp.rebalance_task().max_bytes());
    resultReq.set_migrated_objects(1);
    resultReq.set_target_remain_bytes(MEMORY_CAPACITY - 700);  // fresh: real used = 700
    master::ReportRebalanceResultRspPb resultRsp;
    DS_ASSERT_OK(scheduler.ReportResult(resultReq, resultRsp));
    // Chain ends (no next batch in resultRsp, or it doesn't matter for this test).

    // S2 reports. Schedule's projection uses max(snapshot(100), freshUsedMemory(700)) = 700.
    // headroomToWatermark = 800 - 700 = 100, so maxBytes is capped at 100 (not 300+).
    auto secondSnapshot = MakeSnapshot({
        MakeNode(source1, 900, 100),
        MakeNode(source2, 900, 100),
        MakeNode(target, 100, 900),  // still stale in snapshot
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, source2, secondSnapshot);
    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty()) << "S2 should still get a task";
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), target);
    // maxBytes should be capped by headroomToWatermark = 800 - 700 = 100, not the stale
    // headroom = 800 - 100 = 700. Without freshUsedMemory, maxBytes would be min(400, 700, ...) = 400.
    EXPECT_LE(secondRsp.rebalance_task().max_bytes(), 100ul)
        << "freshUsedMemory should cap maxBytes at headroomToWatermark=100, not stale 700";
}

// Verify freshUsedMemory survives chain-continues (P2-1 fix): when the chain continues,
// ReleaseHeldLocked may erase the futureView_ entry, and operator[] recreates it with
// freshUsedMemory=0. The fix re-applies freshUsedMemory AFTER the chain-continues block.
// This test verifies the fresh signal is present on the surviving/recreated entry so a
// concurrent source's Schedule sees the real (higher) target usage.
TEST_F(MemoryRebalanceSchedulerTest, FreshUsedMemorySurvivesChainContinue)
{
    const std::string source1 = "127.0.0.1:9401";
    const std::string source2 = "127.0.0.1:9402";
    const std::string target = "127.0.0.1:1401";

    // totalBudget: (900-100)/2=400, headroom=800-100=700, avail=900; budget=400.
    // batch1 maxBytes = min(400, 700, 900, 300MiB) = 400.
    // batch2: freshUsed=capacity-remain=1000-500=500, remaining=400-400=0 -> no chain.
    // To get a chain-continues scenario, totalBudget must exceed REBALANCE_MAX_BYTES_PER_TASK
    // (300 MiB) so batch1 is capped at 300MiB but remaining budget > 0 triggers a next batch.
    // With CAP = 1G: gap=(900M-100M)/2=400M, budget=min(400M,700M,900M)=400M,
    // batch1=min(400M,700M,900M,314M)=314M, remaining=400M-314M=86M>0 -> chain continues.
    const uint64_t BIG_CAP = 1'000'000'000;
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(source1, 900'000'000, 100'000'000, true, BIG_CAP, BIG_CAP),
        MakeNode(source2, 900'000'000, 100'000'000, true, BIG_CAP, BIG_CAP),
        MakeNode(target, 100'000'000, 900'000'000, true, BIG_CAP, BIG_CAP),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source1, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), target);

    // batch1 succeeds. target_remain_bytes shows real used = 500M (remain = 500M).
    // freshUsed = 1G - 500M = 500M.
    // remaining = 400M - 314M = 86M > 0 -> chain continues.
    // ReleaseHeldLocked erases entry (held==inflight), operator[] recreates with freshUsedMemory=0.
    // P2-1 fix: line 306 re-applies freshUsedMemory = 500M on the recreated entry.
    master::ReportRebalanceResultReqPb resultReq;
    resultReq.set_task_id(firstRsp.rebalance_task().task_id());
    resultReq.set_source_worker(source1);
    resultReq.set_target_worker(target);
    resultReq.set_status(master::REBALANCE_TASK_SUCCEEDED);
    resultReq.set_migrated_bytes(firstRsp.rebalance_task().max_bytes());
    resultReq.set_migrated_objects(1);
    resultReq.set_target_remain_bytes(BIG_CAP - 500'000'000);  // fresh: real used = 500M
    master::ReportRebalanceResultRspPb resultRsp;
    DS_ASSERT_OK(scheduler.ReportResult(resultReq, resultRsp));

    // Chain continued: a next batch task should be present.
    ASSERT_TRUE(resultRsp.has_next_rebalance_task())
        << "Chain should continue (remaining budget > 0)";
    EXPECT_EQ(resultRsp.next_rebalance_task().target_worker(), target);

    // Verify freshUsedMemory survived the ReleaseHeldLocked + recreate.
    EXPECT_EQ(GetFreshUsedMemory(scheduler, target), 500'000'000ul)
        << "freshUsedMemory must survive chain-continue (P2-1 fix)";

    // Now S2 reports. Schedule should use freshUsedMemory=500M in projection, not stale 100M.
    auto secondSnapshot = MakeSnapshot({
        MakeNode(source1, 900'000'000, 100'000'000, true, BIG_CAP, BIG_CAP),
        MakeNode(source2, 900'000'000, 100'000'000, true, BIG_CAP, BIG_CAP),
        MakeNode(target, 100'000'000, 900'000'000, true, BIG_CAP, BIG_CAP),  // stale
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, source2, secondSnapshot);
    // S2 should target the same target but with capped maxBytes due to freshUsedMemory.
    if (!secondRsp.rebalance_task().task_id().empty()) {
        EXPECT_LE(secondRsp.rebalance_task().max_bytes(), 300'000'000ul)
            << "freshUsedMemory (500M) should cap S2's maxBytes via headroomToWatermark";
    }
}

// ============================================================================
// Review fix C1: totalBudget must use the effective target view (max(snapshot,
// freshUsedMemory)), not the stale snapshot. Without this fix, the first batch
// is correctly sized by CalculateTaskBytesLocked (which uses max(snapshot, fresh)),
// but FillTaskFromPairLocked computes totalBudget from the stale snapshot — so
// the chain continues past the convergence point and reverses source/target.
// ============================================================================
TEST_F(MemoryRebalanceSchedulerTest, EffectiveTargetUsedBoundsTotalBudgetWithFreshSignal)
{
    // Small scale (MEMORY_CAPACITY=1000): 300MB cap does not bind, so maxBytes = gap/2.
    // S1 chain: 1 batch of 400, freshUsed=500. S2's totalBudget must use effectiveTargetUsed
    // (500), not stale snapshot (100). totalBudget=200 (not 400), so after 1 batch of 200
    // remaining=0 -> no next batch. Without fix: totalBudget=400, remaining=200, next batch
    // dispatched -> source/target reversed.
    const std::string source1 = "127.0.0.1:9501";
    const std::string source2 = "127.0.0.1:9502";
    const std::string target = "127.0.0.1:1501";

    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(source1, 900, 100),
        MakeNode(source2, 900, 100),
        MakeNode(target, 100, 900),
    });

    // S1: maxBytes = min((900-100)/2=400, 800-100=700, 900, 314M) = 400. totalBudget = 400.
    auto s1Rsp = ScheduleAndGetRsp(scheduler, source1, snapshot);
    ASSERT_FALSE(s1Rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(s1Rsp.rebalance_task().max_bytes(), 400ul);

    // S1 batch: migrated=400 (task.max_bytes), target used=100+400=500, remain=500.
    // freshUsed=500. remaining=400-400=0 -> chain ends. freshUsedMemory=500.
    master::ReportRebalanceResultRspPb s1Result;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(s1Rsp.rebalance_task(), MEMORY_CAPACITY - 500), s1Result));
    EXPECT_FALSE(s1Result.has_next_rebalance_task());
    EXPECT_EQ(GetFreshUsedMemory(scheduler, target), 500ul);

    // S2: effectiveTargetUsed = max(100, 500) = 500.
    // totalBudget (with fix) = min((900-500)/2=200, 800-500=300, 500) = 200.
    // totalBudget (without fix) = min((900-100)/2=400, 800-100=700, 500) = 400.
    auto s2Rsp = ScheduleAndGetRsp(scheduler, source2, snapshot);
    ASSERT_FALSE(s2Rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(s2Rsp.rebalance_task().target_worker(), target);
    // maxBytes = min(200, 300, 500, 314M) = 200.
    EXPECT_EQ(s2Rsp.rebalance_task().max_bytes(), 200ul);

    // S2 batch: migrated=200, target used=500+200=700, remain=300.
    // remaining (with fix) = 200-200 = 0 -> no next batch.
    // remaining (without fix) = 400-200 = 200 -> next batch dispatched (over-migration).
    master::ReportRebalanceResultRspPb s2Result;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(s2Rsp.rebalance_task(), MEMORY_CAPACITY - 700), s2Result));
    EXPECT_FALSE(s2Result.has_next_rebalance_task())
        << "totalBudget must use effectiveTargetUsed (500), not stale snapshot (100); "
           "without fix, remaining=200 would dispatch another batch and reverse source/target";
}

// ============================================================================
// Review fix C2a: a partial batch (migrated < max_bytes) with a valid
// target_remain_bytes must still update freshUsedMemory so concurrent sources
// see the real target usage. Without this fix, the early return at the partial
// check skips the freshUsedMemory assignment entirely.
// ============================================================================
TEST_F(MemoryRebalanceSchedulerTest, PartialBatchUpdatesFreshOverlay)
{
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";

    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900 * MB, ONE_GB - 900 * MB, true, ONE_GB, ONE_GB),
        MakeNode(target, 100 * MB, ONE_GB - 100 * MB, true, ONE_GB, ONE_GB),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    // maxBytes = min((900M-100M)/2=400M, 800M-100M=700M, 900M, 314M) = 314M (300MB cap).
    EXPECT_EQ(firstRsp.rebalance_task().max_bytes(), 300 * MB);

    // Partial batch: only 100MB of 300MB migrated (candidates exhausted).
    // Target remain = ONE_GB - 200MB (used = 100M+100M = 200M).
    // freshUsed = ONE_GB - remain = 200MB.
    master::ReportRebalanceResultReqPb req;
    req.set_task_id(firstRsp.rebalance_task().task_id());
    req.set_source_worker(source);
    req.set_target_worker(target);
    req.set_status(master::REBALANCE_TASK_SUCCEEDED);
    req.set_migrated_bytes(100 * MB);  // partial: 100MB of 300MB
    req.set_migrated_objects(1);
    req.set_target_remain_bytes(ONE_GB - 200 * MB);  // fresh: real used = 200MB
    master::ReportRebalanceResultRspPb resultRsp;
    DS_ASSERT_OK(scheduler.ReportResult(req, resultRsp));

    // Chain must NOT continue (partial batch).
    EXPECT_FALSE(resultRsp.has_next_rebalance_task())
        << "partial batch must not continue chain";

    // freshUsedMemory must be set to 200MB (the fix moves the assignment before the partial return).
    // Without fix: freshUsedMemory stays 0 because the partial return skips the assignment.
    EXPECT_EQ(GetFreshUsedMemory(scheduler, target), 200 * MB)
        << "partial batch with valid target_remain must still update freshUsedMemory";
}

// ============================================================================
// Review fix C2b: out-of-order results from concurrent sources must not regress
// the freshUsedMemory overlay. When a later-arriving result carries a lower
// freshUsed (e.g. target had eviction, or the observation predates another
// source's landing), max() merge prevents the overlay from going backwards.
// Without this fix, unconditional assignment overwrites the higher value with
// a lower one, letting a third source reuse non-existent headroom.
// ============================================================================
TEST_F(MemoryRebalanceSchedulerTest, OutOfOrderResultDoesNotRegressFreshOverlay)
{
    // S1 chain sets freshUsed=500. S2's batch reports a LOWER freshUsed=400 (e.g. target had
    // eviction, or the observation predates S1's landing). max() must prevent regression.
    const std::string source1 = "127.0.0.1:9601";
    const std::string source2 = "127.0.0.1:9602";
    const std::string target = "127.0.0.1:1601";

    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(source1, 900, 100),
        MakeNode(source2, 900, 100),
        MakeNode(target, 100, 900),
    });

    // S1: maxBytes=400, totalBudget=400. Batch succeeds, freshUsed=500.
    auto s1Rsp = ScheduleAndGetRsp(scheduler, source1, snapshot);
    ASSERT_FALSE(s1Rsp.rebalance_task().task_id().empty());
    master::ReportRebalanceResultRspPb s1Result;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(s1Rsp.rebalance_task(), MEMORY_CAPACITY - 500), s1Result));
    EXPECT_FALSE(s1Result.has_next_rebalance_task());
    ASSERT_EQ(GetFreshUsedMemory(scheduler, target), 500ul);

    // S2 schedules. effectiveTargetUsed = max(100, 500) = 500.
    // maxBytes = min((900-500)/2=200, 800-500=300, 500, 314M) = 200.
    auto s2Rsp = ScheduleAndGetRsp(scheduler, source2, snapshot);
    ASSERT_FALSE(s2Rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(s2Rsp.rebalance_task().max_bytes(), 200ul);

    // S2 batch: migrated=200, but target reports used=400 (lower than existing overlay 500).
    // freshUsed = 400 < 500. Without fix: unconditional overwrite -> 400 (regression).
    // With fix: max(500, 400) = 500 (no regression).
    master::ReportRebalanceResultRspPb s2Result;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(s2Rsp.rebalance_task(), MEMORY_CAPACITY - 400), s2Result));

    EXPECT_EQ(GetFreshUsedMemory(scheduler, target), 500ul)
        << "out-of-order/lower observation must not regress freshUsedMemory (max merge)";
}

// ============================================================================
// Review fix C3: when freshUsedMemory is set, ReleaseSnapshotHoldsLocked must
// not release held based solely on the master receipt timestamp. A stale
// ResourceReport (sampled before migration) can arrive at master after the
// migration result, so timestamp > holdSinceMs but usedMemory < freshUsedMemory.
// The fix adds: if freshUsedMemory > 0, also require snapshot.usedMemory >=
// freshUsedMemory before releasing.
// ============================================================================
TEST_F(MemoryRebalanceSchedulerTest, StaleSnapshotDoesNotReleaseHeldWhenUsedMemoryBelowFresh)
{
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";

    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900, 100),
        MakeNode(target, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    // maxBytes = min((900-100)/2=400, 800-100=700, 900, 314M) = 400.
    EXPECT_EQ(firstRsp.rebalance_task().max_bytes(), 400ul);

    // Batch succeeds. target_remain = 500 (used = 100+400 = 500).
    // freshUsed = 500. Chain ends (remaining = 400-400 = 0).
    master::ReportRebalanceResultRspPb resultRsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(firstRsp.rebalance_task(), MEMORY_CAPACITY - 500, 400), resultRsp));
    EXPECT_FALSE(resultRsp.has_next_rebalance_task());
    ASSERT_EQ(GetFreshUsedMemory(scheduler, target), 500ul);
    ASSERT_TRUE(HasPendingRelease(scheduler));

    // Stale snapshot: timestamp advanced past holdSinceMs (master received the
    // report after the migration result), but usedMemory = 100 (sampled before
    // migration — still shows stale-low). Without fix: held released + freshUsed
    // cleared → Schedule sees stale-low target → over-migration. With fix: retained.
    const uint64_t holdTs = GetHoldTs(scheduler, target);
    NodeInfo staleTarget(target, 900, true, holdTs + 1, 100, MEMORY_CAPACITY, MEMORY_CAPACITY);
    auto staleSnapshot = MakeSnapshot({
        MakeNode(source, 900, 100),
        staleTarget,
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, source, staleSnapshot);

    EXPECT_TRUE(HasPendingRelease(scheduler))
        << "stale snapshot (usedMemory < freshUsedMemory) must NOT release held";
    EXPECT_EQ(GetFreshUsedMemory(scheduler, target), 500ul)
        << "freshUsedMemory must be retained when snapshot usedMemory hasn't caught up";
}

// ============================================================================
// Review fix C3 positive case: when the snapshot's usedMemory has caught up
// to freshUsedMemory, the snapshot is proven fresh — held is safe to release
// and freshUsedMemory is cleared. This verifies the fix doesn't block the
// normal release path.
// ============================================================================
TEST_F(MemoryRebalanceSchedulerTest, FreshSnapshotReleasesHeldWhenUsedMemoryCoversFresh)
{
    const std::string source = "127.0.0.1:9200";
    const std::string target = "127.0.0.1:1000";

    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900, 100),
        MakeNode(target, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(firstRsp.rebalance_task().max_bytes(), 400ul);

    master::ReportRebalanceResultRspPb resultRsp;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(firstRsp.rebalance_task(), MEMORY_CAPACITY - 500, 400), resultRsp));
    ASSERT_EQ(GetFreshUsedMemory(scheduler, target), 500ul);
    ASSERT_TRUE(HasPendingRelease(scheduler));

    // Fresh snapshot: timestamp advanced AND usedMemory = 500 >= freshUsedMemory = 500.
    // The snapshot has caught up — release held and clear freshUsedMemory.
    const uint64_t holdTs = GetHoldTs(scheduler, target);
    NodeInfo freshTarget(target, 500, true, holdTs + 1, 500, MEMORY_CAPACITY, MEMORY_CAPACITY);
    auto freshSnapshot = MakeSnapshot({
        MakeNode(source, 900, 100),
        freshTarget,
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, source, freshSnapshot);

    EXPECT_FALSE(HasPendingRelease(scheduler))
        << "fresh snapshot (usedMemory >= freshUsedMemory) must release held";
    EXPECT_EQ(GetFreshUsedMemory(scheduler, target), 0ul)
        << "freshUsedMemory must be cleared after fresh snapshot releases held";
}

// ============================================================================
// Finding 1 fix: ReleaseHeldLocked in the chain-continues path clears
// freshUsedMemory to 0. Without saving/restoring the max-merged value, a
// concurrent source's higher observation is lost when another source's chain
// continues. This test constructs that exact scenario:
//
// 1. S1 chain (3 batches) ends with freshUsed=800M.
// 2. S2 batch 1 succeeds with freshUsed=700M (lower — eviction or out-of-order).
//    max() at line 285 correctly merges to 800M.
//    But S2's chain continues → ReleaseHeldLocked clears freshUsed to 0.
//    Re-apply only restores max(0, 700M) = 700M → regression!
// 3. Without the save/restore fix, freshUsedMemory drops to 700M.
//    With the fix (savedFreshUsed + 3-way max), it stays at 800M.
//
// Requires 2B scale so the 300MB cap binds maxBytes (314.5M) while totalBudget
// exceeds it (350M), making S2's chain continue.
// ============================================================================
TEST_F(MemoryRebalanceSchedulerTest, ChainContinuePreservesConcurrentSourceFreshOverlay)
{
    // 2B scale: 300MB cap binds maxBytes (314M) while totalBudget exceeds it, so S2's chain
    // continues — triggering ReleaseHeldLocked which clears freshUsedMemory. The fix saves
    // the max-merged value before ReleaseHeldLocked and restores it via 3-way max.
    constexpr uint64_t BIG = 2'000'000'000;
    const std::string source1 = "127.0.0.1:9701";
    const std::string source2 = "127.0.0.1:9702";
    const std::string target = "127.0.0.1:1701";

    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(source1, 1'700'000'000, BIG - 1'700'000'000, true, BIG, BIG),
        MakeNode(source2, 1'700'000'000, BIG - 1'700'000'000, true, BIG, BIG),
        MakeNode(target, 100'000'000, BIG - 100'000'000, true, BIG, BIG),
    });

    // S1: 85% >= 80% source threshold. maxBytes = min(800M, 1.5B, 1.9B, 314M) = 314M (cap).
    // totalBudget = min(800M, 1.5B, 1.9B) = 800M. Chain: 3 batches (314M + 314M + 170.9M).
    auto s1Rsp = ScheduleAndGetRsp(scheduler, source1, snapshot);
    ASSERT_FALSE(s1Rsp.rebalance_task().task_id().empty());
    ASSERT_EQ(s1Rsp.rebalance_task().target_worker(), target);

    // S1 batch 1: target used = 100M + 314,572,800 = 414,572,800. remaining = 485,427,200.
    master::ReportRebalanceResultRspPb s1r1;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(s1Rsp.rebalance_task(), BIG - 414'572'800), s1r1));
    ASSERT_TRUE(s1r1.has_next_rebalance_task());

    // S1 batch 2: target used = 414,572,800 + 314,572,800 = 729,145,600. remaining = 170,854,400.
    master::ReportRebalanceResultRspPb s1r2;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(s1r1.next_rebalance_task(), BIG - 729'145'600), s1r2));
    ASSERT_TRUE(s1r2.has_next_rebalance_task());

    // S1 batch 3: target used = 729,145,600 + 170,854,400 = 900,000,000. remaining = 0. Chain ends.
    // freshUsedMemory = 900M.
    master::ReportRebalanceResultRspPb s1r3;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(s1r2.next_rebalance_task(), BIG - 900'000'000), s1r3));
    EXPECT_FALSE(s1r3.has_next_rebalance_task());
    ASSERT_EQ(GetFreshUsedMemory(scheduler, target), 900'000'000ul)
        << "S1 chain should end with freshUsedMemory=900M";

    // S2: effectiveTargetUsed = max(100M, 900M) = 900M (85% vs 45%, gap=40% >= 20%).
    // usageGapBytes = (1.7B-900M)/2 = 400M. maxBytes = min(400M, 700M, 1.1B, 314M) = 314M (cap).
    // totalBudget = min(400M, 700M, 1.1B) = 400M. remaining = 85,427,200 > 0 → chain continues.
    auto s2Rsp = ScheduleAndGetRsp(scheduler, source2, snapshot);
    ASSERT_FALSE(s2Rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(s2Rsp.rebalance_task().target_worker(), target);

    // S2 batch 1: reports remain=2B-800M (freshUsed=800M < existing 900M — eviction or out-of-order).
    // max() at line 285: max(900M, 800M) = 900M ✓.
    // Chain continues → ReleaseHeldLocked clears freshUsed to 0 ✗ (without fix).
    // Re-apply: max(0, 800M) = 800M ✗ (without fix) vs max(0, 900M, 800M) = 900M ✓ (with fix).
    master::ReportRebalanceResultRspPb s2r1;
    DS_ASSERT_OK(scheduler.ReportResult(
        MakeFreshResultReq(s2Rsp.rebalance_task(), BIG - 800'000'000), s2r1));
    ASSERT_TRUE(s2r1.has_next_rebalance_task())
        << "S2 chain should continue (totalBudget=400M > maxBytes=314M)";

    // Key assertion: freshUsedMemory must not regress from 900M to 800M when the chain
    // continues and ReleaseHeldLocked clears the field. The save/restore fix preserves it.
    EXPECT_EQ(GetFreshUsedMemory(scheduler, target), 900'000'000ul)
        << "ReleaseHeldLocked in chain-continues must not lose concurrent source's max-merged "
           "freshUsed; savedFreshUsed restore prevents the regression";
}

}  // namespace ut
}  // namespace datasystem
