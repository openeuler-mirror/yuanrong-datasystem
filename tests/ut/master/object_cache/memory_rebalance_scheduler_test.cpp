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
#include "ut/common.h"

DS_DECLARE_bool(enable_memory_rebalance);
DS_DECLARE_uint32(rebalance_source_usage_percent);
DS_DECLARE_uint32(rebalance_usage_gap_percent);
DS_DECLARE_uint32(rebalance_cooldown_s);
DS_DECLARE_uint32(rebalance_task_report_grace_ms);
DS_DECLARE_uint32(data_migrate_rate_limit_mb);

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

        FLAGS_enable_memory_rebalance = true;
        FLAGS_rebalance_source_usage_percent = 70;
        FLAGS_rebalance_usage_gap_percent = 30;
        FLAGS_rebalance_cooldown_s = 60;
        FLAGS_rebalance_task_report_grace_ms = 300;
        FLAGS_data_migrate_rate_limit_mb = 500;
    }

    void TearDown() override
    {
        FLAGS_enable_memory_rebalance = oldEnableMemoryRebalance_;
        FLAGS_rebalance_source_usage_percent = oldSourceUsagePercent_;
        FLAGS_rebalance_usage_gap_percent = oldUsageGapPercent_;
        FLAGS_rebalance_cooldown_s = oldCooldownS_;
        FLAGS_rebalance_task_report_grace_ms = oldTaskTimeoutS_;
        FLAGS_data_migrate_rate_limit_mb = oldDataMigrateRate_;
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

private:
    bool oldEnableMemoryRebalance_ = false;
    uint32_t oldSourceUsagePercent_ = 0;
    uint32_t oldUsageGapPercent_ = 0;
    uint32_t oldCooldownS_ = 0;
    uint32_t oldTaskTimeoutS_ = 0;
    uint32_t oldDataMigrateRate_ = 0;
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
    auto estimated_transfer_ms = ((rsp.rebalance_task().max_bytes() + rate_bytes_per_sec - 1) / rate_bytes_per_sec) * MS_PER_SECOND;
    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().deadline_ms() - rsp.rebalance_task().create_time_ms(),
              estimated_transfer_ms * TRANSFER_TIME_MULTIPLIER + FLAGS_rebalance_task_report_grace_ms);
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

TEST_F(MemoryRebalanceSchedulerTest, SuccessfulResultDoesNotCooldownSourceTargetPair)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());

    master::ReportRebalanceResultRspPb reportRsp;
    auto successReq = MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED);
    DS_ASSERT_OK(scheduler.ReportResult(successReq, reportRsp));

    auto secondRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().source_worker(), WORKER_92);
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), WORKER_10);
}

}  // namespace ut
}  // namespace datasystem
