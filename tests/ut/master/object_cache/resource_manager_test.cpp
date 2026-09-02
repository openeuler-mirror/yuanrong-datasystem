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
 * Description: Test ResourceManager scheduling snapshots.
 */

#include <algorithm>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/master/resource_manager.h"
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
constexpr uint64_t MEMORY_LIMIT = 1'000;
// Huge interval + skip flag disable every background WorkerThread swap so tests can control
// read/write snapshot placement deterministically.
constexpr int64_t DISABLE_BG_INTERVAL_MS = 60 * 60 * 1'000;  // 1h

master::ResourceReportReqPb MakeResourceReq(const std::string &worker, uint64_t usedMemory,
                                             uint64_t availableMemory, uint64_t memoryLimit = MEMORY_LIMIT)
{
    master::ResourceReportReqPb req;
    auto *stat = req.mutable_stat();
    stat->set_address(worker);
    stat->set_available_memory(availableMemory);
    stat->set_used_memory(usedMemory);
    stat->set_memory_capacity(usedMemory + availableMemory);
    stat->set_memory_limit(memoryLimit);
    stat->set_is_ready(true);
    return req;
}

// Push one worker report through ResourceManager::ReportResource and return the response.
master::ResourceReportRspPb Report(ResourceManager &rm, const std::string &worker, uint64_t usedMemory,
                                    uint64_t availableMemory)
{
    master::ResourceReportRspPb rsp;
    auto req = MakeResourceReq(worker, usedMemory, availableMemory);
    auto rc = rm.ReportResource(req, rsp);
    EXPECT_TRUE(rc.IsOk()) << "ReportResource failed for " << worker << ": " << rc.ToString();
    return rsp;
}

bool HasRebalanceTask(const master::ResourceReportRspPb &rsp)
{
    return !rsp.rebalance_task().task_id().empty();
}

// Rebalance watermark in bytes == source trigger threshold (the flag is dual-role: it gates
// source selection AND caps the target migration ceiling). Mirrors the scheduler's SubOrZero.
uint64_t WatermarkBytes(uint64_t memoryLimit = MEMORY_LIMIT)
{
    return memoryLimit * FLAGS_rebalance_source_usage_percent / 100;
}

uint64_t SubOrZero(uint64_t lhs, uint64_t rhs)
{
    return lhs > rhs ? lhs - rhs : 0;
}
}  // namespace

class TestResourceManager : public ResourceManager {
public:
    using ResourceManager::SwitchSnapshots;
    using ResourceManager::ClearWriteSnapshot;
};

class ResourceManagerTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        oldEnable_ = FLAGS_enable_memory_rebalance;
        oldSourcePct_ = FLAGS_rebalance_source_usage_percent;
        oldGapPct_ = FLAGS_rebalance_usage_gap_percent;
        oldGraceMs_ = FLAGS_rebalance_task_report_grace_ms;
        oldRateMb_ = FLAGS_data_migrate_rate_limit_mb;
        oldNodeDeadS_ = FLAGS_node_dead_timeout_s;

        FLAGS_enable_memory_rebalance = true;
        FLAGS_rebalance_source_usage_percent = 80;
        FLAGS_rebalance_usage_gap_percent = 20;
        // Large grace keeps the active task from expiring mid-test (deterministic, no real timeout).
        FLAGS_rebalance_task_report_grace_ms = 60'000;
        // 1 MB/s so the transfer-time estimate stays bounded for any realistic max_bytes.
        FLAGS_data_migrate_rate_limit_mb = 1;
        FLAGS_node_dead_timeout_s = 0;  // TTL = max(0, HOLD_TTL_MIN_S) = 60s; flaky-safe

        // Disable background swaps BEFORE constructing ResourceManager so the WorkerThread's first
        // iteration already sees the injects (no first-swap race).
        DS_ASSERT_OK(inject::Set("ResourceManager.skipBackgroundSwap", "call()"));
        DS_ASSERT_OK(inject::Set("ResourceManager.setInterval", FormatString("call(%ld)", DISABLE_BG_INTERVAL_MS)));
        rm_ = std::make_unique<TestResourceManager>();
    }

    void TearDown() override
    {
        (void)inject::Clear("ResourceManager.BuildLatestSnapshot.beforeReadMerge");
        rm_.reset();  // join WorkerThread before clearing injects it might still read
        (void)inject::Clear("ResourceManager.skipBackgroundSwap");
        (void)inject::Clear("ResourceManager.setInterval");
        FLAGS_enable_memory_rebalance = oldEnable_;
        FLAGS_rebalance_source_usage_percent = oldSourcePct_;
        FLAGS_rebalance_usage_gap_percent = oldGapPct_;
        FLAGS_rebalance_task_report_grace_ms = oldGraceMs_;
        FLAGS_data_migrate_rate_limit_mb = oldRateMb_;
        FLAGS_node_dead_timeout_s = oldNodeDeadS_;
        CommonTest::TearDown();
    }

    Status ReportRebalanceSuccess(ResourceManager &rm, const master::RebalanceTaskPb &task)
    {
        master::ReportRebalanceResultReqPb req;
        req.set_task_id(task.task_id());
        req.set_source_worker(task.source_worker());
        req.set_target_worker(task.target_worker());
        req.set_status(master::REBALANCE_TASK_SUCCEEDED);
        req.set_migrated_bytes(task.max_bytes());
        req.set_migrated_objects(1);
        master::ReportRebalanceResultRspPb rsp;
        return rm.ReportRebalanceResult(req, rsp);
    }

protected:
    std::unique_ptr<TestResourceManager> rm_;

private:
    bool oldEnable_ = false;
    uint32_t oldSourcePct_ = 0;
    uint32_t oldGapPct_ = 0;
    uint32_t oldGraceMs_ = 0;
    uint32_t oldRateMb_ = 0;
    uint32_t oldNodeDeadS_ = 0;
};

// Regression for the steady-state residual of issue #685.
//
// Without merging both buffers, the hold is released when the target reports its post-receive memory
// (write-side fresh), but Schedule still reads the lagged readSnapshot_ which shows the target
// stale-low. The next source then re-picks the just-received target on the stale-low projection
// and over-migrates it.
//
// Merging the two buffers by timestamp lets Schedule see the target's real (higher) post-receive
// usage, so the midpoint budget is correct and stays below the eviction trigger line.
//
// Timeline (MEMORY_LIMIT = 1000, source threshold = source_pct, gap = 20%):
//   T1 initial  used=100  (10%)  -- pickable low target
//   T1 received used=700  (70%)  -- still < source threshold so its own report does NOT trigger a schedule
//                                   (not a source), hence readSnapshot_ stays stale-low for it
//   S2 / S3      used=920  (92%)  -- high sources
//
//   1. Report T1@100   -> writeSnapshot_[T1]=100 (stale-low)
//   2. Report S2@920   -> merged snapshot is fresh -> Schedule assigns S2->T1
//                         (max_bytes=(920-100)/2=410, projected 100+0+410=510 < triggerLine ok)
//      SwitchSnapshots -> readSnapshot_[T1]=100, writeSnapshot_ becomes the previous empty read buffer
//   3. ReportResult(S2->T1, SUCCEEDED) -> hold T1 (inflight[T1]=410 held)
//   4. Report T1@700   -> writeSnapshot_[T1]=700 fresh; T1 reports, releases its own hold
//                         (inflight->0); T1 is 70% < source threshold so NOT a source -> NO swap; readSnapshot_[T1]
//                         stays stale-low (100). THIS is the exposure window.
//   5. Report S3@920   -> merged snapshot uses T1@700 fresh -> Schedule: S3->T1
//                         midpoint=(920-700)/2=110, headroomToWatermark=watermark-700=100 (binds, < midpoint),
//                         targetAvail=300-0=300; max_bytes=100, projected=700+100=800=watermark (not past it).
//
// Assertion: S3 gets a rebalance task targeting T1 with max_bytes=100. The target watermark ceiling
// binds because headroomToWatermark=100 < midpoint=110 — this keeps the #685 target-ceiling coverage
// that would be lost if T1 sat at 600 (headroom=200 > midpoint=160, midpoint would bind instead).
// Without the merge, readSnapshot_[T1]=100 stale -> max_bytes=(920-100)/2=410 -> T1 (really 700)
// pushed to 1110 > triggerLine(1000) -> over-migration.)
TEST_F(ResourceManagerTest, MergedSnapshotGivesFreshProjectionAndCorrectBudget)
{
    const std::string t1 = "127.0.0.1:1010";
    const std::string s2 = "127.0.0.1:9201";
    const std::string s3 = "127.0.0.1:9202";

    // 1. T1 reports low -> only lands in writeSnapshot_.
    (void)Report(*rm_, t1, 100, 900);

    // 2. S2 reports high -> merged snapshot contains T1+S2 -> S2->T1 assigned.
    auto s2Rsp = Report(*rm_, s2, 920, 80);
    ASSERT_TRUE(HasRebalanceTask(s2Rsp)) << "S2 should get a rebalance task targeting T1";
    ASSERT_EQ(s2Rsp.rebalance_task().target_worker(), t1);
    ASSERT_EQ(s2Rsp.rebalance_task().max_bytes(), 410ul);
    rm_->SwitchSnapshots();

    // 3. S2's executor reports success -> hold T1 (held charge stays until T1 reports fresh).
    DS_ASSERT_OK(ReportRebalanceSuccess(*rm_, s2Rsp.rebalance_task()));

    // Sleep 2ms so T1's report timestamp strictly exceeds the hold timestamp (set at step 3).
    // Without this, the timestamps are equal and ReleaseReporterHoldsLocked's <= guard skips
    // the release, leaving the held charge on T1 and blocking S3->T1 with maxBytes=0.
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    // 4. T1 reports its real post-receive usage (700, 70%). It is not a source (70% < source threshold) so
    //    The read buffer stays stale-low (100), while the write buffer advances past the hold time.
    (void)Report(*rm_, t1, 700, 300);

    // 5. S3 reports high -> the merged snapshot selects writeSnapshot_'s T1@700. Schedule's
    //    projection for S3->T1: the rebalance goal is
    //    the midpoint gap (920-700)/2=110, but the target watermark ceiling caps the batch at
    //    headroomToWatermark = watermark-700 = 100 (< midpoint=110), so T1 reaches the watermark
    //    (not past it). The available clamp (300-0=300 > headroom) does not bind. Without the
    //    merge, readSnapshot_[T1] would still be stale-low (100) and the watermark cap
    //    would be watermark-100 = 700 (looser, > midpoint=410), so the midpoint would bind instead
    //    and over-migrate T1 — the fresh snapshot is what makes the watermark ceiling bind tightly.
    auto s3Rsp = Report(*rm_, s3, 920, 80);
    ASSERT_TRUE(HasRebalanceTask(s3Rsp)) << "S3 should get a rebalance task targeting T1";
    EXPECT_EQ(s3Rsp.rebalance_task().target_worker(), t1);
    EXPECT_EQ(s3Rsp.rebalance_task().max_bytes(),
              std::min({ 110ul, SubOrZero(WatermarkBytes(), 700), 300ul }));
}

TEST_F(ResourceManagerTest, HighUsageReportDoesNotLoseTargetsSplitAcrossBuffers)
{
    const std::string source = "127.0.0.1:9200";
    const std::string target1 = "127.0.0.1:1010";
    const std::string target2 = "127.0.0.1:1020";
    const std::string target3 = "127.0.0.1:1030";

    (void)Report(*rm_, source, 100, 900);
    (void)Report(*rm_, target1, 100, 900);
    (void)Report(*rm_, target2, 100, 900);
    (void)Report(*rm_, target3, 100, 900);
    rm_->SwitchSnapshots();

    auto rsp = Report(*rm_, source, 900, 100);

    ASSERT_TRUE(HasRebalanceTask(rsp));
    EXPECT_EQ(rsp.rebalance_task().source_worker(), source);
    EXPECT_NE(rsp.rebalance_task().target_worker(), source);
    EXPECT_EQ(rsp.stats_size(), 4);
}

TEST_F(ResourceManagerTest, SnapshotBuildDoesNotBlockConcurrentReportWhileReadingOldSnapshot)
{
    constexpr char source[] = "127.0.0.1:9200";
    constexpr char target[] = "127.0.0.1:1010";
    constexpr char concurrentWorker[] = "127.0.0.1:1020";
    constexpr char injectPoint[] = "ResourceManager.BuildLatestSnapshot.beforeReadMerge";

    (void)Report(*rm_, source, 100, 900);
    (void)Report(*rm_, target, 100, 900);
    rm_->SwitchSnapshots();

    DS_ASSERT_OK(inject::Set(injectPoint, "1*pause()"));
    auto highUsageReport = std::async(std::launch::async, [&] {
        master::ResourceReportRspPb rsp;
        return rm_->ReportResource(MakeResourceReq(source, 900, 100), rsp);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (inject::GetExecuteCount(injectPoint) == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (inject::GetExecuteCount(injectPoint) == 0) {
        DS_EXPECT_OK(inject::Clear(injectPoint));
        DS_EXPECT_OK(highUsageReport.get());
        FAIL() << "Snapshot build did not reach the read-snapshot merge";
    }

    auto concurrentReport = std::async(std::launch::async, [&] {
        master::ResourceReportRspPb rsp;
        return rm_->ReportResource(MakeResourceReq(concurrentWorker, 100, 900), rsp);
    });
    const bool reportCompleted = concurrentReport.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    DS_EXPECT_OK(inject::Clear(injectPoint));
    DS_EXPECT_OK(highUsageReport.get());
    DS_EXPECT_OK(concurrentReport.get());
    EXPECT_TRUE(reportCompleted) << "Snapshot build held the write snapshot lock while copying the read snapshot";
}

// Regression guard: ClearWriteSnapshot must not purge alive workers when node_dead_timeout_s is
// small (e.g. 5s in production). The 30s resource-report cycle far exceeds the heartbeat-based
// dead timeout; without a floor, ClearWriteSnapshot wipes all entries that are more than 5s old,
// leaving the scheduling snapshot empty and breaking rebalance. The fix adds a 60s floor
// (SNAPSHOT_CLEAR_MIN_S), mirroring HOLD_TTL_MIN_S in the rebalance scheduler.
//
// Fixture sets FLAGS_node_dead_timeout_s = 0 and skips background swaps, so the test can
// deterministically call ClearWriteSnapshot + SwitchSnapshots in sequence.
//
// Without the fix: deadTimestamp = now - 0 = now. After 2ms sleep, all worker timestamps
//   are strictly < now → all purged → snapshot empty → no rebalance task.
// With the fix: deadTimestamp = now - max(0, 60)s = now - 60s. Worker timestamps are
//   2ms old, well within 60s → all kept → snapshot has 3 targets → rebalance task assigned.
TEST_F(ResourceManagerTest, ClearWriteSnapshotDoesNotPurgeAliveWorkersWithSmallDeadTimeout)
{
    const std::string source = "127.0.0.1:9200";
    const std::string target1 = "127.0.0.1:1010";
    const std::string target2 = "127.0.0.1:1020";
    const std::string target3 = "127.0.0.1:1030";

    (void)Report(*rm_, target1, 100, 900);
    (void)Report(*rm_, target2, 200, 800);
    (void)Report(*rm_, target3, 300, 700);

    rm_->SwitchSnapshots();
    rm_->SwitchSnapshots();

    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    rm_->ClearWriteSnapshot();

    rm_->SwitchSnapshots();

    auto rsp = Report(*rm_, source, 900, 100);

    ASSERT_TRUE(HasRebalanceTask(rsp))
        << "ClearWriteSnapshot purged alive workers; snapshot had no targets for rebalance";
    EXPECT_NE(rsp.rebalance_task().source_worker(), rsp.rebalance_task().target_worker());
    EXPECT_EQ(rsp.stats_size(), 4) << "Snapshot should have 3 targets + 1 source";
}
}  // namespace ut
}  // namespace datasystem
