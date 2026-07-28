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
 * Description: Test ResourceManager swap-on-trigger (issue #685 steady-state residual).
 */

#include <memory>
#include <string>

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
DS_DECLARE_uint32(rebalance_cooldown_s);
DS_DECLARE_uint32(rebalance_task_report_grace_ms);
DS_DECLARE_uint32(data_migrate_rate_limit_mb);
DS_DECLARE_uint32(node_dead_timeout_s);

using namespace datasystem::master;

namespace datasystem {
namespace ut {
namespace {
constexpr uint64_t MEMORY_LIMIT = 1'000;
// Huge interval + skip flag disable every background WorkerThread swap so the only snapshot
// promotion during a test is the swap-on-trigger inside ReportResource. This is what makes the
// regression assertion deterministic: without skip, a racing background swap could freshen
// readSnapshot_ before the source reports and mask the bug the test must catch.
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
}  // namespace

class ResourceManagerTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        oldEnable_ = FLAGS_enable_memory_rebalance;
        oldSourcePct_ = FLAGS_rebalance_source_usage_percent;
        oldGapPct_ = FLAGS_rebalance_usage_gap_percent;
        oldCooldownS_ = FLAGS_rebalance_cooldown_s;
        oldGraceMs_ = FLAGS_rebalance_task_report_grace_ms;
        oldRateMb_ = FLAGS_data_migrate_rate_limit_mb;
        oldNodeDeadS_ = FLAGS_node_dead_timeout_s;

        FLAGS_enable_memory_rebalance = true;
        FLAGS_rebalance_source_usage_percent = 70;
        FLAGS_rebalance_usage_gap_percent = 30;
        FLAGS_rebalance_cooldown_s = 60;
        // Large grace keeps the active task from expiring mid-test (deterministic, no real timeout).
        FLAGS_rebalance_task_report_grace_ms = 60'000;
        // 1 MB/s so the transfer-time estimate stays bounded for any realistic max_bytes.
        FLAGS_data_migrate_rate_limit_mb = 1;
        FLAGS_node_dead_timeout_s = 0;  // TTL = max(0, HOLD_TTL_MIN_S) = 60s; flaky-safe

        // Disable background swaps BEFORE constructing ResourceManager so the WorkerThread's first
        // iteration already sees the injects (no first-swap race).
        DS_ASSERT_OK(inject::Set("ResourceManager.skipBackgroundSwap", "call()"));
        DS_ASSERT_OK(inject::Set("ResourceManager.setInterval", FormatString("call(%ld)", DISABLE_BG_INTERVAL_MS)));
        rm_ = std::make_unique<ResourceManager>();
    }

    void TearDown() override
    {
        rm_.reset();  // join WorkerThread before clearing injects it might still read
        (void)inject::Clear("ResourceManager.skipBackgroundSwap");
        (void)inject::Clear("ResourceManager.setInterval");
        FLAGS_enable_memory_rebalance = oldEnable_;
        FLAGS_rebalance_source_usage_percent = oldSourcePct_;
        FLAGS_rebalance_usage_gap_percent = oldGapPct_;
        FLAGS_rebalance_cooldown_s = oldCooldownS_;
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
    std::unique_ptr<ResourceManager> rm_;

private:
    bool oldEnable_ = false;
    uint32_t oldSourcePct_ = 0;
    uint32_t oldGapPct_ = 0;
    uint32_t oldCooldownS_ = 0;
    uint32_t oldGraceMs_ = 0;
    uint32_t oldRateMb_ = 0;
    uint32_t oldNodeDeadS_ = 0;
};

// Regression for the steady-state residual of issue #685 (swap-on-trigger fix).
//
// Without swap-on-trigger, the hold is released when the target reports its post-receive memory
// (write-side fresh), but Schedule still reads the lagged readSnapshot_ which shows the target
// stale-low. The next source then re-picks the just-received target on the stale-low projection
// and over-migrates it.
//
// With swap-on-trigger, the source's report promotes writeSnapshot_ into readSnapshot_ BEFORE
// Schedule builds its projection, so the target is seen at its real (higher) post-receive usage
// and the midpoint budget is correct (smaller), keeping projected usage below the eviction
// trigger line.
//
// Timeline (MEMORY_LIMIT = 1000, source threshold = 70%, gap = 30%):
//   T1 initial  used=100  (10%)  -- pickable low target
//   T1 received used=600  (60%)  -- still < 70% so its own report does NOT trigger a schedule
//                                   (not a source), hence readSnapshot_ stays stale-low for it
//   S2 / S3      used=920  (92%)  -- high sources
//
//   1. Report T1@100   -> writeSnapshot_[T1]=100 (stale-low)
//   2. Report S2@920   -> swap-on-trigger -> readSnapshot_ fresh -> Schedule assigns S2->T1
//                         (max_bytes=(920-100)/2=410, projected 100+0+410=510 < triggerLine ok)
//   3. ReportResult(S2->T1, SUCCEEDED) -> hold T1 (inflight[T1]=410 held)
//   4. Report T1@600   -> writeSnapshot_[T1]=600 fresh; T1 reports, releases its own hold
//                         (inflight->0); T1 is 60% < 70% so NOT a source -> NO swap; readSnapshot_[T1]
//                         stays stale-low (100). THIS is the exposure window.
//   5. Report S3@920   -> swap-on-trigger -> readSnapshot_[T1]=600 fresh -> Schedule: S3->T1
//                         projected = 600+0+160 = 760 < triggerLine(1000) -> dispatch, max_bytes=160.
//
// Assertion: S3 gets a rebalance task targeting T1 with max_bytes=160. (Without swap-on-trigger,
// readSnapshot_[T1]=100 stale -> max_bytes=(920-100)/2=410 -> T1 (really 600) pushed to 1010
// > triggerLine -> over-migration.)
TEST_F(ResourceManagerTest, SwapOnTriggerGivesFreshProjectionAndCorrectBudget)
{
    const std::string t1 = "127.0.0.1:1010";
    const std::string s2 = "127.0.0.1:9201";
    const std::string s3 = "127.0.0.1:9202";

    // 1. T1 reports low -> only lands in writeSnapshot_ (no source, no swap-on-trigger).
    (void)Report(*rm_, t1, 100, 900);

    // 2. S2 reports high -> swap-on-trigger promotes T1+S2 into readSnapshot_ -> S2->T1 assigned.
    auto s2Rsp = Report(*rm_, s2, 920, 80);
    ASSERT_TRUE(HasRebalanceTask(s2Rsp)) << "S2 should get a rebalance task targeting T1";
    ASSERT_EQ(s2Rsp.rebalance_task().target_worker(), t1);
    ASSERT_EQ(s2Rsp.rebalance_task().max_bytes(), 410ul);

    // 3. S2's executor reports success -> hold T1 (held charge stays until T1 reports fresh).
    DS_ASSERT_OK(ReportRebalanceSuccess(*rm_, s2Rsp.rebalance_task()));

    // Sleep 2ms so T1's report timestamp strictly exceeds the hold timestamp (set at step 3).
    // Without this, the timestamps are equal and ReleaseReporterHoldsLocked's <= guard skips
    // the release, leaving the held charge on T1 and blocking S3->T1 with maxBytes=0.
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    // 4. T1 reports its real post-receive usage (600, 60%). It is not a source (60% < 70%) so
    //    NO swap-on-trigger fires here -- readSnapshot_[T1] stays stale-low (100) and the held
    //    charge is released (write-side report advances past hold time).
    (void)Report(*rm_, t1, 600, 400);

    // 5. S3 reports high -> swap-on-trigger promotes writeSnapshot_ (T1 now 600 fresh) into
    //    readSnapshot_ BEFORE Schedule. Schedule's projection for S3->T1 = 600+0+160 = 760
    //    < triggerLine(1000) -> T1 dispatched with max_bytes=160. The 70% ceiling is removed;
    //    the available clamp (400-0=400 > 160) does not bind; the midpoint (920-600)/2=160
    //    binds. Without swap-on-trigger, readSnapshot_[T1] would still be stale-low (100) and
    //    S3->T1 max_bytes would be (920-100)/2=410, over-migrating T1 to 600+410=1010 > limit.
    auto s3Rsp = Report(*rm_, s3, 920, 80);
    ASSERT_TRUE(HasRebalanceTask(s3Rsp)) << "S3 should get a rebalance task targeting T1";
    EXPECT_EQ(s3Rsp.rebalance_task().target_worker(), t1);
    EXPECT_EQ(s3Rsp.rebalance_task().max_bytes(), 160ul);
}
}  // namespace ut
}  // namespace datasystem
