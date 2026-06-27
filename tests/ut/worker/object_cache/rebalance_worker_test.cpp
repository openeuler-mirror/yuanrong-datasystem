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
 * Description: Test worker-side memory rebalance components.
 */

#include <condition_variable>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/object_cache/rebalance_candidate_provider.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/rebalance_executor.h"
#include "eviction_manager_common.h"
#include "ut/common.h"

using namespace datasystem::object_cache;
using namespace datasystem::worker;

namespace datasystem {
namespace ut {
namespace {
constexpr uint64_t MB = 1024ul * 1024ul;
const HostPort LOCAL_ADDR("127.0.0.1", 31501);
const HostPort MASTER_ADDR("127.0.0.1", 31500);
const std::string TARGET_ADDR = "127.0.0.1:31502";

master::RebalanceTaskPb MakeTask(const std::string &taskId, uint64_t maxBytes)
{
    master::RebalanceTaskPb task;
    task.set_task_id(taskId);
    task.set_source_worker(LOCAL_ADDR.ToString());
    task.set_target_worker(TARGET_ADDR);
    task.set_max_bytes(maxBytes);
    auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    task.set_create_time_ms(nowMs);
    task.set_deadline_ms(nowMs + 60 * 1000);
    return task;
}
}  // namespace

class RebalanceCandidateProviderTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        objectTable_ = std::make_shared<ObjectTable>();
        allocator = memory::Allocator::Instance();
        allocator->Init(maxMemorySize);
        akSkManager_ = std::make_shared<AkSkManager>(0);
        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, LOCAL_ADDR, MASTER_ADDR);
        globalRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        DS_ASSERT_OK(evictionManager_->Init(globalRefTable_, akSkManager_));
    }

    void TearDown() override
    {
        evictionManager_.reset();
        globalRefTable_.reset();
        akSkManager_.reset();
        objectTable_.reset();
        CommonTest::TearDown();
    }

protected:
    void CreateAndAdd(const std::string &objectKey, uint64_t dataSize, bool primaryCopy = true)
    {
        DS_ASSERT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE, primaryCopy));
        evictionManager_->Add(objectKey);
    }

    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
};

// Verifies that the candidate provider scans from the oldest eviction-list entry and stops once the selected bytes
// reach the requested target. This protects the LRU-style rebalance candidate order and batch-size boundary.
TEST_F(RebalanceCandidateProviderTest, SelectCandidatesFromOldestUntilTargetBytes)
{
    CreateAndAdd("oldest", 10 * MB);
    CreateAndAdd("middle", 20 * MB);
    CreateAndAdd("newest", 30 * MB);

    RebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    DS_ASSERT_OK(provider.Select(25 * MB, 10, candidates));

    ASSERT_EQ(candidates.size(), size_t(2));
    EXPECT_EQ(candidates["oldest"], 10 * MB);
    EXPECT_EQ(candidates["middle"], 20 * MB);
    EXPECT_EQ(candidates.count("newest"), size_t(0));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("oldest"));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("middle"));
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("newest"));
}

// Verifies that unstable candidates are skipped: non-primary copies must not be migrated, and objects already marked
// rebalancing must not be selected again by another rebalance batch.
TEST_F(RebalanceCandidateProviderTest, SkipNonPrimaryAndAlreadyRebalancingObjects)
{
    CreateAndAdd("non_primary", 10 * MB, false);
    CreateAndAdd("already_rebalancing", 20 * MB);
    CreateAndAdd("candidate", 30 * MB);
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("already_rebalancing"));

    RebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    DS_ASSERT_OK(provider.Select(10 * MB, 10, candidates));

    ASSERT_EQ(candidates.size(), size_t(1));
    EXPECT_EQ(candidates["candidate"], 30 * MB);
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("non_primary"));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("already_rebalancing"));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("candidate"));
}

// Verifies the eviction/rebalance exclusion path. When eviction sees an object already owned by rebalance, it should
// skip the object for this eviction round and re-add it with READD_COUNTER instead of repeatedly selecting it.
TEST_F(RebalanceCandidateProviderTest, EvictionSkipsRebalancingObjectAndReaddsIt)
{
    CreateAndAdd("rebalancing_object", 10 * MB);
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("rebalancing_object"));

    evictionManager_->EvictionTaskForTest(maxMemorySize);

    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get("rebalancing_object", entry));
    std::vector<EvictionList::Node> readdNodes;
    EvictionList::Node oldest;
    DS_ASSERT_OK(evictionManager_->GetAllObjectsInfo(readdNodes, oldest));
    ASSERT_EQ(readdNodes.size(), size_t(1));
    EXPECT_EQ(readdNodes[0].objectKey, "rebalancing_object");
    EXPECT_EQ(readdNodes[0].curCounter, READD_COUNTER);
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("rebalancing_object"));
}

class RebalanceExecutorTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        objectTable_ = std::make_shared<ObjectTable>();
        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, LOCAL_ADDR, MASTER_ADDR);
        RebalanceExecutorConfig config{ LOCAL_ADDR, nullptr, nullptr, objectTable_, evictionManager_, nullptr };
        executor_ = std::make_unique<RebalanceExecutor>(std::move(config));
    }

protected:
    struct ReportRecord {
        master::RebalanceTaskStatusPb status = master::REBALANCE_TASK_INIT;
        uint64_t migratedBytes = 0;
        uint64_t migratedObjects = 0;
        uint64_t failedObjects = 0;
        std::string failedReason;
    };

    void InstallHooks(const std::vector<std::unordered_map<std::string, uint64_t>> &batches,
                      const std::vector<RebalanceExecutor::MigrateResult> &results)
    {
        batches_ = batches;
        results_ = results;
        selectIndex_ = 0;
        migrateIndex_ = 0;
        reportCount_ = 0;
        reports_.clear();
        reportThreadIds_.clear();
        executor_->SetTestHooks(
            [this](uint64_t maxBytes, std::unordered_map<std::string, uint64_t> &candidates) {
                (void)maxBytes;
                if (selectIndex_ >= batches_.size()) {
                    return Status(K_NOT_FOUND, "no more test candidates");
                }
                candidates = batches_[selectIndex_++];
                return Status::OK();
            },
            [this](const master::RebalanceTaskPb &, const HostPort &, const std::vector<std::string> &objectKeys) {
                migratedObjectKeys_.push_back(objectKeys);
                if (migrateIndex_ >= results_.size()) {
                    RebalanceExecutor::MigrateResult result;
                    result.status = Status(K_RUNTIME_ERROR, "missing test result");
                    return result;
                }
                return results_[migrateIndex_++];
            },
            [this](const master::RebalanceTaskPb &, master::RebalanceTaskStatusPb status, uint64_t migratedBytes,
                   uint64_t migratedObjects, uint64_t failedObjects, const std::string &failedReason) {
                std::lock_guard<std::mutex> lock(mutex_);
                reports_.push_back({ status, migratedBytes, migratedObjects, failedObjects, failedReason });
                reportThreadIds_.push_back(std::this_thread::get_id());
                ++reportCount_;
                cv_.notify_all();
            });
    }

    bool WaitReports(size_t expected)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(3), [this, expected] { return reportCount_ >= expected; });
    }

    bool WaitTaskDone()
    {
        constexpr int retryTimes = 100;
        constexpr int sleepMs = 10;
        for (int i = 0; i < retryTimes; ++i) {
            if (!executor_->IsRunningForTest()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
        }
        return false;
    }

    RebalanceExecutor::MigrateResult MakeMigrateResult(const std::vector<std::string> &successIds,
                                                       const Status &status = Status::OK())
    {
        RebalanceExecutor::MigrateResult result;
        result.status = status;
        for (const auto &objectKey : successIds) {
            result.successIds.emplace(ImmutableString(objectKey));
        }
        return result;
    }

    RebalanceExecutor::MigrateResult MakeFailedMigrateResult(const std::vector<std::string> &successIds,
                                                             const std::vector<std::string> &failedIds,
                                                             const Status &status)
    {
        auto result = MakeMigrateResult(successIds, status);
        for (const auto &objectKey : failedIds) {
            result.failedIds.emplace(ImmutableString(objectKey));
        }
        return result;
    }

    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    std::unique_ptr<RebalanceExecutor> executor_;
    std::vector<std::unordered_map<std::string, uint64_t>> batches_;
    std::vector<RebalanceExecutor::MigrateResult> results_;
    std::vector<std::vector<std::string>> migratedObjectKeys_;
    size_t selectIndex_ = 0;
    size_t migrateIndex_ = 0;
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t reportCount_ = 0;
    std::vector<ReportRecord> reports_;
    std::vector<std::thread::id> reportThreadIds_;
};

// Verifies the successful executor path: selected objects migrate successfully, the executor reports SUCCEEDED, clears
// its running task state, and releases the rebalancing marks for the batch.
TEST_F(RebalanceExecutorTest, SubmitReportsSucceededAndClearsRunningState)
{
    InstallHooks({ { { "obj1", 10 }, { "obj2", 20 } } }, { MakeMigrateResult({ "obj1", "obj2" }) });
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj1"));
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj2"));

    executor_->Submit(MakeTask("success-task", 30));

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(30));
    EXPECT_EQ(reports_[0].migratedObjects, uint64_t(2));
    EXPECT_EQ(reports_[0].failedObjects, uint64_t(0));
    EXPECT_FALSE(executor_->IsRunningForTest());
    EXPECT_TRUE(executor_->GetRunningTaskIdForTest().empty());
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("obj1"));
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("obj2"));
    ASSERT_EQ(migratedObjectKeys_.size(), size_t(1));
    EXPECT_EQ(migratedObjectKeys_[0].size(), size_t(2));
}

// Verifies the failed executor path: a migration error or failed object causes FAILED to be reported with accurate
// migrated/failed counters, and the executor still clears running state and rebalancing marks.
TEST_F(RebalanceExecutorTest, SubmitReportsFailedAndClearsRunningState)
{
    InstallHooks({ { { "obj1", 10 }, { "obj2", 20 } } },
                 { MakeFailedMigrateResult({ "obj1" }, { "obj2" }, Status(K_RUNTIME_ERROR, "migrate failed")) });
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj1"));
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj2"));

    executor_->Submit(MakeTask("failed-task", 30));

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_FAILED);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(10));
    EXPECT_EQ(reports_[0].migratedObjects, uint64_t(1));
    EXPECT_EQ(reports_[0].failedObjects, uint64_t(1));
    EXPECT_NE(reports_[0].failedReason.find("migrate failed"), std::string::npos);
    EXPECT_FALSE(executor_->IsRunningForTest());
    EXPECT_TRUE(executor_->GetRunningTaskIdForTest().empty());
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("obj1"));
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("obj2"));
}

// Verifies multi-batch execution. If one batch does not satisfy task.max_bytes, the executor keeps selecting and
// migrating additional batches until the requested byte target is reached.
TEST_F(RebalanceExecutorTest, SubmitRunsMultipleBatchesUntilTargetBytesReached)
{
    InstallHooks({ { { "obj1", 30 }, { "obj2", 30 } }, { { "obj3", 40 } } },
                 { MakeMigrateResult({ "obj1", "obj2" }), MakeMigrateResult({ "obj3" }) });

    executor_->Submit(MakeTask("multi-batch-task", 100));

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(100));
    EXPECT_EQ(reports_[0].migratedObjects, uint64_t(3));
    EXPECT_EQ(reports_[0].failedObjects, uint64_t(0));
    ASSERT_EQ(migratedObjectKeys_.size(), size_t(2));
    EXPECT_EQ(migratedObjectKeys_[0].size(), size_t(2));
    EXPECT_EQ(migratedObjectKeys_[1].size(), size_t(1));
}

// Exhausting local candidates after a successful batch is a valid partial completion. The migrated data remains
// effective, so the result must not be reported as FAILED and put the source/target pair into cooldown.
TEST_F(RebalanceExecutorTest, SubmitReportsSucceededWhenCandidatesExhaustedAfterPartialMigration)
{
    InstallHooks({ { { "obj1", 30 }, { "obj2", 20 } } }, { MakeMigrateResult({ "obj1", "obj2" }) });

    executor_->Submit(MakeTask("partial-task", 100));

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(50));
    EXPECT_EQ(reports_[0].migratedObjects, uint64_t(2));
    EXPECT_EQ(reports_[0].failedObjects, uint64_t(0));
    EXPECT_TRUE(reports_[0].failedReason.empty());
    EXPECT_EQ(selectIndex_, size_t(1));
    EXPECT_EQ(migrateIndex_, size_t(1));
}

// Verifies deadline handling. An expired task should be reported as EXPIRED, should not call the migration hook, and
// should always clear executor running state.
TEST_F(RebalanceExecutorTest, SubmitExpiredTaskReportsExpiredAndClearsRunningState)
{
    InstallHooks({}, {});
    auto task = MakeTask("expired-task", 10);
    task.set_deadline_ms(static_cast<uint64_t>(GetSteadyClockTimeStampMs()) - 1);

    executor_->Submit(task);

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_EXPIRED);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(0));
    EXPECT_EQ(reports_[0].migratedObjects, uint64_t(0));
    EXPECT_EQ(reports_[0].failedObjects, uint64_t(0));
    EXPECT_TRUE(migratedObjectKeys_.empty());
    EXPECT_FALSE(executor_->IsRunningForTest());
    EXPECT_TRUE(executor_->GetRunningTaskIdForTest().empty());
}

// Verifies duplicate-source protection. When the source worker is already running another task, a new task is rejected
// with FAILED and the existing running task id is left intact.
TEST_F(RebalanceExecutorTest, BusySourceReportsFailedWithoutReplacingRunningTask)
{
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });
    executor_->SetRunningForTest(true, "running-task");
    auto submitThreadId = std::this_thread::get_id();

    executor_->Submit(MakeTask("new-task", 10));

    ASSERT_TRUE(WaitReports(1));
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_FAILED);
    EXPECT_NE(reports_[0].failedReason.find("busy"), std::string::npos);
    ASSERT_EQ(reportThreadIds_.size(), size_t(1));
    EXPECT_NE(reportThreadIds_[0], submitThreadId);
    EXPECT_TRUE(executor_->IsRunningForTest());
    EXPECT_EQ(executor_->GetRunningTaskIdForTest(), "running-task");
}

}  // namespace ut
}  // namespace datasystem
