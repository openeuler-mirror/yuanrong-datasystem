/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Test SpillFileManager and eviction manager.
 */

#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "ut/common.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/log/log.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "eviction_manager_common.h"

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_file_open_limit);
DS_DECLARE_uint64(spill_file_max_size_mb);
DS_DECLARE_uint64(spill_size_limit);

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {

class SpillEvictionTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp()
    {
        CommonTest::SetUp();

        const uint64_t memSize = 1024ul * 1024ul * 1024ul;
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(memSize);

        FLAGS_spill_directory = "./spill" + GetStringUuid();
        FLAGS_spill_size_limit = maxSize_;
        FLAGS_v = 1;

        objectTable_ = std::make_shared<ObjectTable>();
        gRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        akSkManager_ = std::make_shared<AkSkManager>(0);
        DS_ASSERT_OK(akSkManager_->SetClientAkSk(accessKey_, secretKey_));
        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, workerAddr_, workerAddr_);
        DS_ASSERT_OK(evictionManager_->Init(gRefTable_, akSkManager_));

        handler_ = WorkerOcSpill::Instance();
        LOG_IF_ERROR(inject::Set("worker.Spill.Sync", "return()"), "set inject point failed");
    }

    void CreateObjects(const std::string &prefix, uint64_t dataSize, uint32_t count,
                       WriteMode mode = WriteMode::NONE_L2_CACHE)
    {
        for (uint32_t i = 0; i < count; ++i) {
            std::string objectKey = prefix + GetModeName(mode) + std::to_string(i);
            DS_ASSERT_OK(CreateObject(objectKey, dataSize, mode));
        }
    }

    void SpillEvictMock(const std::string &objectKey, const void *buffer, uint64_t size, bool retry = false,
                        bool evictable = true)
    {
        evictionManager_->TryEvictSpilledObjects(size);
        if (retry) {
            Status rc;
            const size_t maxRetryCount = 5;
            for (size_t i = 0; i < maxRetryCount; ++i) {
                rc = WorkerOcSpill::Instance()->Spill(objectKey, buffer, size, evictable);
                if (rc.GetCode() == StatusCode::K_NO_SPACE) {
                    constexpr int sleepTimes = 50'000;
                    usleep(sleepTimes);
                } else {
                    break;
                }
            }
            DS_ASSERT_OK(rc);
        } else {
            DS_ASSERT_OK(WorkerOcSpill::Instance()->Spill(objectKey, buffer, size, evictable));
        }
    }

    void TestEvictionTraceAggregatorClassifiesSuccessAndFailedKeys()
    {
        WorkerOcEvictionManager::EvictionTraceAggregator aggregator;
        WorkerOcEvictionManager::EvictionTrace successTrace("spill_task_success");
        successTrace.action = WorkerOcEvictionManager::Action::SPILL;
        successTrace.rc = Status::OK();
        successTrace.AddObjectKeySize("success_key_0", 1);
        successTrace.AddObjectKeySize("success_key_1", 1);
        aggregator.Add(successTrace);

        WorkerOcEvictionManager::EvictionTrace failedTrace("spill_task_failed");
        failedTrace.action = WorkerOcEvictionManager::Action::SPILL;
        failedTrace.rc = Status(K_RUNTIME_ERROR, "spill failed");
        failedTrace.AddObjectKeySize("failed_key_0", 1);
        aggregator.Add(failedTrace);

        const auto &summaries = aggregator.GetSummaries();
        const auto &summary = summaries.at(WorkerOcEvictionManager::Action::SPILL);
        ASSERT_EQ(summary.successKeys.size(), size_t(2));
        ASSERT_EQ(summary.failedKeys.size(), size_t(1));
        ASSERT_TRUE(std::find(summary.successKeys.begin(), summary.successKeys.end(), "success_key_0")
                    != summary.successKeys.end());
        ASSERT_TRUE(std::find(summary.successKeys.begin(), summary.successKeys.end(), "success_key_1")
                    != summary.successKeys.end());
        ASSERT_TRUE(std::find(summary.failedKeys.begin(), summary.failedKeys.end(), "failed_key_0")
                    != summary.failedKeys.end());
    }

    void TestEvictionTraceAggregatorTreatsWholeTraceAsFailedWhenNoFailedKeyDetails()
    {
        WorkerOcEvictionManager::EvictionTraceAggregator aggregator;
        WorkerOcEvictionManager::EvictionTrace trace("failed_task");
        trace.action = WorkerOcEvictionManager::Action::DELETE;
        trace.rc = Status(K_RUNTIME_ERROR, "delete failed");
        trace.AddObjectKeySize("failed_key_0", 1);
        trace.AddObjectKeySize("failed_key_1", 1);

        aggregator.Add(trace);

        const auto &summaries = aggregator.GetSummaries();
        const auto &summary = summaries.at(WorkerOcEvictionManager::Action::DELETE);
        ASSERT_EQ(summary.successKeys.size(), size_t(0));
        ASSERT_EQ(summary.failedKeys.size(), size_t(2));
        ASSERT_TRUE(std::find(summary.failedKeys.begin(), summary.failedKeys.end(), "failed_key_0")
                    != summary.failedKeys.end());
        ASSERT_TRUE(std::find(summary.failedKeys.begin(), summary.failedKeys.end(), "failed_key_1")
                    != summary.failedKeys.end());
    }

    void TestEvictionTraceAggregatorFlushesWhenKeyCountExceedsThreshold()
    {
        WorkerOcEvictionManager::EvictionTraceAggregator aggregator;
        constexpr size_t threshold = 32;
        for (size_t i = 0; i < threshold; ++i) {
            WorkerOcEvictionManager::EvictionTrace trace("key_" + std::to_string(i));
            trace.action = WorkerOcEvictionManager::Action::FREE_MEMORY;
            trace.rc = Status::OK();
            trace.AddObjectKeySize(trace.taskId, 1);
            aggregator.Add(trace);
        }

        const auto &summaries = aggregator.GetSummaries();
        const auto &summary = summaries.at(WorkerOcEvictionManager::Action::FREE_MEMORY);
        ASSERT_EQ(summary.successKeys.size(), size_t(0));
        ASSERT_EQ(summary.failedKeys.size(), size_t(0));
    }

    void TestEvictionTraceAggregatorSkipsRetryableStatusAndUsesActionName()
    {
        WorkerOcEvictionManager::EvictionTraceAggregator aggregator;
        WorkerOcEvictionManager::EvictionTrace trace("retry_key");
        trace.action = WorkerOcEvictionManager::Action::SPILL;
        trace.rc = Status(K_TRY_AGAIN, "retry later");
        trace.AddObjectKeySize("retry_key", 1);

        aggregator.Add(trace);

        ASSERT_EQ(aggregator.GetSummaries().count(WorkerOcEvictionManager::Action::SPILL), size_t(0));
        ASSERT_EQ(WorkerOcEvictionManager::GetActionName(WorkerOcEvictionManager::Action::SPILL), "spill");
    }

    void TestPrimaryEndLifeReserveDuplicateAndLimit()
    {
        constexpr size_t pendingLimit = 64;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> tasks;
        tasks.reserve(pendingLimit);
        for (size_t i = 0; i < pendingLimit; ++i) {
            tasks.emplace_back(WorkerOcEvictionManager::PrimaryEndLifeTask{
                "primary_end_life_pending_" + std::to_string(i), i, CacheType::MEMORY });
        }
        bool accepted = false;
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(tasks.front(), accepted));
        ASSERT_TRUE(accepted);
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(tasks.front(), accepted));
        ASSERT_FALSE(accepted);
        for (size_t i = 1; i < pendingLimit; ++i) {
            DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(tasks[i], accepted));
            ASSERT_TRUE(accepted);
        }
        WorkerOcEvictionManager::PrimaryEndLifeTask overflow{ "primary_end_life_overflow", 0, CacheType::MEMORY };
        ASSERT_EQ(evictionManager_->ReservePrimaryEndLifeTask(overflow, accepted).GetCode(), K_TRY_AGAIN);
        for (const auto &task : tasks) {
            evictionManager_->ClearPrimaryEndLifePending(task);
        }
    }

    void TestPrimaryEndLifePrepareSkipsWhenAtLowWater()
    {
        const std::string objectKey = "primary_end_life_low_water";
        constexpr uint64_t objectSize = 1024;
        DS_ASSERT_OK(CreateObject(objectKey, objectSize, WriteMode::NONE_L2_CACHE_EVICT));
        WorkerOcEvictionManager::PrimaryEndLifeTask task{ objectKey, 1, CacheType::MEMORY };
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> tasks{ task };
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> candidates;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> skippedTasks;
        DS_ASSERT_OK(evictionManager_->PreparePrimaryEndLifeCandidates(tasks, candidates, skippedTasks));
        ASSERT_TRUE(candidates.empty());
        ASSERT_EQ(skippedTasks.size(), size_t(1));
        ASSERT_EQ(skippedTasks.front().objectKey, objectKey);
        DS_ASSERT_OK(DeleteObject(objectKey));
    }

    void TestPrimaryEndLifePrepareUsesForegroundNeedSize()
    {
        const std::string objectKey = "primary_end_life_need_size";
        constexpr uint64_t objectSize = 1024;
        const uint64_t triggerDelta = 1;
        DS_ASSERT_OK(CreateObject(objectKey, objectSize, WriteMode::NONE_L2_CACHE_EVICT));
        auto needSize = evictionManager_->GetLowWaterMark(CacheType::MEMORY) + triggerDelta;
        WorkerOcEvictionManager::PrimaryEndLifeTask task{ objectKey, 1, CacheType::MEMORY, needSize };
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> tasks{ task };
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> candidates;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> skippedTasks;

        DS_ASSERT_OK(evictionManager_->PreparePrimaryEndLifeCandidates(tasks, candidates, skippedTasks));

        ASSERT_EQ(candidates.size(), size_t(1));
        ASSERT_TRUE(skippedTasks.empty());
        WorkerOcEvictionManager::UnlockPrimaryEndLifeCandidates(candidates);
        DS_ASSERT_OK(DeleteObject(objectKey));
    }

    void TestDeleteAllCopyMetaResultCollectsPerKeyFailures()
    {
        const std::string okKey = "primary_end_life_batch_ok";
        const std::string failedKey = "primary_end_life_batch_failed";
        const std::string outdatedKey = "primary_end_life_batch_outdated";

        master::DeleteAllCopyMetaRspPb rsp;
        rsp.add_failed_object_keys(failedKey);
        rsp.add_outdated_objs(outdatedKey);
        std::unordered_set<std::string> failedKeys;
        DS_ASSERT_OK(WorkerOcEvictionManager::CollectDeleteAllCopyMetaResult(rsp, failedKeys));
        ASSERT_EQ(failedKeys.count(okKey), size_t(0));
        ASSERT_EQ(failedKeys.count(failedKey), size_t(1));
        ASSERT_EQ(failedKeys.count(outdatedKey), size_t(1));

        rsp.mutable_last_rc()->set_error_code(K_RUNTIME_ERROR);
        rsp.mutable_last_rc()->set_error_msg("delete failed");
        failedKeys.clear();
        ASSERT_EQ(WorkerOcEvictionManager::CollectDeleteAllCopyMetaResult(rsp, failedKeys).GetCode(),
                  K_RUNTIME_ERROR);
        ASSERT_EQ(failedKeys.count(failedKey), size_t(1));
        ASSERT_EQ(failedKeys.count(outdatedKey), size_t(1));

        const std::string redirectKey = "primary_end_life_batch_redirect";
        const std::string noMetaKey = "primary_end_life_batch_no_meta";
        master::DeleteAllCopyMetaRspPb redirectRsp;
        redirectRsp.add_objs_without_meta(noMetaKey);
        auto *redirectInfo = redirectRsp.add_info();
        redirectInfo->add_change_meta_ids(redirectKey);
        failedKeys.clear();
        DS_ASSERT_OK(WorkerOcEvictionManager::CollectDeleteAllCopyMetaResult(redirectRsp, failedKeys));
        ASSERT_EQ(failedKeys.count(redirectKey), size_t(1));
        ASSERT_EQ(failedKeys.count(noMetaKey), size_t(1));

        master::DeleteAllCopyMetaRspPb movingRsp;
        movingRsp.set_meta_is_moving(true);
        failedKeys.clear();
        ASSERT_EQ(WorkerOcEvictionManager::CollectDeleteAllCopyMetaResult(movingRsp, failedKeys).GetCode(),
                  K_TRY_AGAIN);
    }

    void TestEvictObjectEndLifeKeepsLocalObjectWhenTaskAlreadyPending()
    {
        const std::string objectKey = "primary_end_life_evict_object_pending";
        constexpr uint64_t objectSize = 1024;
        DS_ASSERT_OK(CreateObject(objectKey, objectSize, WriteMode::NONE_L2_CACHE_EVICT));
        auto needSize = evictionManager_->GetLowWaterMark(CacheType::MEMORY) + 1;
        WorkerOcEvictionManager::PrimaryEndLifeTask task{ objectKey, 1, CacheType::MEMORY, needSize };
        bool accepted = false;
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(task, accepted));
        ASSERT_TRUE(accepted);
        evictionManager_->memEvictionList_.Add(objectKey, Q1);
        std::shared_ptr<SafeObjType> entry;
        DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
        ObjectKV objectKV(objectKey, *entry);

        DS_ASSERT_OK(evictionManager_->EvictObject(objectKV, WorkerOcEvictionManager::Action::END_LIFE, nullptr,
                                                  CacheType::MEMORY, needSize));

        ASSERT_TRUE(objectTable_->Contains(objectKey));
        ASSERT_FALSE(evictionManager_->memEvictionList_.Exist(objectKey));
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(task, accepted));
        ASSERT_FALSE(accepted);
        evictionManager_->ClearPrimaryEndLifePending(task);
        DS_ASSERT_OK(DeleteObject(objectKey));
    }

    void TestPrimaryEndLifeFinishClearsPendingAndReaddsOnlyOnFailure()
    {
        WorkerOcEvictionManager::PrimaryEndLifeTask failedTask{ "primary_end_life_finish_failed", 1,
                                                                CacheType::MEMORY };
        bool accepted = false;
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(failedTask, accepted));
        ASSERT_TRUE(accepted);
        evictionManager_->FinishPrimaryEndLifeTask(failedTask, true);
        ASSERT_TRUE(evictionManager_->memEvictionList_.Exist(failedTask.objectKey));
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(failedTask, accepted));
        ASSERT_TRUE(accepted);
        evictionManager_->ClearPrimaryEndLifePending(failedTask);
        (void)evictionManager_->memEvictionList_.Erase(failedTask.objectKey);

        WorkerOcEvictionManager::PrimaryEndLifeTask metaDeletedTask{ "primary_end_life_finish_meta_deleted", 1,
                                                                     CacheType::MEMORY };
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(metaDeletedTask, accepted));
        ASSERT_TRUE(accepted);
        metaDeletedTask.metaDeleted = true;
        evictionManager_->FinishPrimaryEndLifeTask(metaDeletedTask, true);
        WorkerOcEvictionManager::PrimaryEndLifeTask retryTask{ metaDeletedTask.objectKey, metaDeletedTask.version,
                                                               metaDeletedTask.cacheType };
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(retryTask, accepted));
        ASSERT_TRUE(accepted);
        ASSERT_TRUE(retryTask.metaDeleted);
        evictionManager_->FinishPrimaryEndLifeTask(retryTask, false);
        WorkerOcEvictionManager::PrimaryEndLifeTask clearedTask{ metaDeletedTask.objectKey, metaDeletedTask.version,
                                                                 metaDeletedTask.cacheType };
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(clearedTask, accepted));
        ASSERT_TRUE(accepted);
        ASSERT_FALSE(clearedTask.metaDeleted);
        evictionManager_->ClearPrimaryEndLifePending(clearedTask);
        (void)evictionManager_->memEvictionList_.Erase(metaDeletedTask.objectKey);

        WorkerOcEvictionManager::PrimaryEndLifeTask successTask{ "primary_end_life_finish_success", 1,
                                                                 CacheType::MEMORY };
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(successTask, accepted));
        ASSERT_TRUE(accepted);
        evictionManager_->FinishPrimaryEndLifeTask(successTask, false);
        ASSERT_FALSE(evictionManager_->memEvictionList_.Exist(successTask.objectKey));
        DS_ASSERT_OK(evictionManager_->ReservePrimaryEndLifeTask(successTask, accepted));
        ASSERT_TRUE(accepted);
        evictionManager_->ClearPrimaryEndLifePending(successTask);
    }

protected:
    std::string GetModeName(WriteMode mode)
    {
        switch (mode) {
            case WriteMode::NONE_L2_CACHE:
                return "none_l2_cache";
            case WriteMode::WRITE_BACK_L2_CACHE:
                return "write_back_l2_cache";
            case WriteMode::WRITE_THROUGH_L2_CACHE:
                return "write_through_l2_cache";
            case WriteMode::NONE_L2_CACHE_EVICT:
                return "none_l2_cache_evict";
            default:
                return "UNKONOWN";
        }
    }

    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    HostPort workerAddr_{ "127.0.0.1", 18481 };
    uint64_t maxSize_ = 64 * 1024ul * 1024ul;
    WorkerOcSpill *handler_;

    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> gRefTable_;
};

TEST_F(SpillEvictionTest, DISABLED_TestSpillEvictableObject)
{
    LOG(INFO) << "Test spill evictable objects and not meets K_NO_SPACE error.";
    std::string prefix = "Obj";
    const uint64_t size = 4 * 1024ul * 1024ul;
    std::vector<uint8_t> spillData(size, 0);
    size_t count = 100;

    CreateObjects(prefix, size, count, WriteMode::WRITE_THROUGH_L2_CACHE);

    for (size_t i = 0; i < count; ++i) {
        std::string objKey = prefix + GetModeName(WriteMode::WRITE_THROUGH_L2_CACHE) + std::to_string(i);
        SpillEvictMock(objKey, spillData.data(), spillData.size());
    }
}

TEST_F(SpillEvictionTest, DISABLED_TestSpillEvictableObjectConcurrently)
{
    LOG(INFO) << "Test spill evictable objects concurrently and not meets K_NO_SPACE error.";
    std::string prefix = "Obj";
    const uint64_t size = 4 * 1024ul * 1024ul;
    std::vector<uint8_t> spillData(size, 0);
    size_t count = 20;
    size_t threadNum = 5;

    CreateObjects(prefix, size, count * threadNum, WriteMode::WRITE_THROUGH_L2_CACHE);

    std::vector<std::thread> threads(threadNum);

    for (size_t i = 0; i < threadNum; ++i) {
        threads[i] = std::thread([this, count, i, &prefix, &spillData]() {
            for (size_t k = i * count; k < (i + 1) * count; ++k) {
                std::string objKey = prefix + GetModeName(WriteMode::WRITE_THROUGH_L2_CACHE) + std::to_string(k);
                SpillEvictMock(objKey, spillData.data(), spillData.size(), true);
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(SpillEvictionTest, DISABLED_TestSpillEvictWriteBackObject)
{
    LOG(INFO) << "Test spill write back objects evict.";
    std::string prefix1 = "Vector";
    const uint64_t size = 4 * 1024ul * 1024ul;
    std::vector<uint8_t> spillData(size, 0);
    size_t count = maxSize_ / size - 1;

    CreateObjects(prefix1, size, count, WriteMode::WRITE_BACK_L2_CACHE);

    for (size_t i = 0; i < count; ++i) {
        std::string objKey = prefix1 + GetModeName(WriteMode::WRITE_BACK_L2_CACHE) + std::to_string(i);
        SpillEvictMock(objKey, spillData.data(), spillData.size());
    }

    // Write back objects has not been written to l2 cache, it can not be evict now, so we now have no space.
    DS_ASSERT_NOT_OK(handler_->Spill("randomrandom", spillData.data(), spillData.size()));

    // Set write back done flags and spill the none l2 cache objects to evict all write back objects.
    for (auto &entry : *objectTable_) {
        entry.second->Get()->stateInfo.SetWriteBackDone(true);
    }

    std::string prefix2 = "Camille";
    CreateObjects(prefix2, size, count, WriteMode::NONE_L2_CACHE);
    for (size_t i = 0; i < count; ++i) {
        std::string objKey = prefix2 + GetModeName(WriteMode::NONE_L2_CACHE) + std::to_string(i);
        SpillEvictMock(objKey, spillData.data(), spillData.size(), true, false);
    }
}

TEST_F(SpillEvictionTest, DISABLED_TestEvictWaterMark)
{
    LOG(INFO) << "Test spill evict water mark.";
    std::string prefix = "Vector";
    const uint64_t size = 4 * 1024ul * 1024ul;
    std::vector<uint8_t> spillData(size, 0);
    size_t count = maxSize_ / size - 1;

    CreateObjects(prefix, size, count, WriteMode::WRITE_THROUGH_L2_CACHE);
    for (size_t i = 0; i < count; ++i) {
        std::string objectKey = prefix + GetModeName(WriteMode::WRITE_THROUGH_L2_CACHE) + std::to_string(i);
        DS_ASSERT_OK(WorkerOcSpill::Instance()->Spill(objectKey, spillData.data(), spillData.size(), true));
    }
    ASSERT_TRUE(WorkerOcSpill::Instance()->IsSpaceExceedHWM());

    std::string newPrefix = "new_added_object";
    CreateObjects(newPrefix, size, 1, WriteMode::NONE_L2_CACHE);
    SpillEvictMock(newPrefix, spillData.data(), spillData.size(), true);

    // Wait for compact process complete.
    const int sleepUs = 10'000;
    usleep(sleepUs);
    ASSERT_FALSE(WorkerOcSpill::Instance()->IsSpaceExceedLWM());
}

TEST_F(SpillEvictionTest, TestEvictionTraceAggregatorClassifiesSuccessAndFailedKeys)
{
    TestEvictionTraceAggregatorClassifiesSuccessAndFailedKeys();
}

TEST_F(SpillEvictionTest, TestEvictionTraceAggregatorTreatsWholeTraceAsFailedWhenNoFailedKeyDetails)
{
    TestEvictionTraceAggregatorTreatsWholeTraceAsFailedWhenNoFailedKeyDetails();
}

TEST_F(SpillEvictionTest, TestEvictionTraceAggregatorFlushesWhenKeyCountExceedsThreshold)
{
    TestEvictionTraceAggregatorFlushesWhenKeyCountExceedsThreshold();
}

TEST_F(SpillEvictionTest, TestEvictionTraceAggregatorSkipsRetryableStatusAndUsesActionName)
{
    TestEvictionTraceAggregatorSkipsRetryableStatusAndUsesActionName();
}

TEST_F(SpillEvictionTest, TestPrimaryEndLifeReserveDuplicateAndLimit)
{
    TestPrimaryEndLifeReserveDuplicateAndLimit();
}

TEST_F(SpillEvictionTest, TestPrimaryEndLifePrepareSkipsWhenAtLowWater)
{
    TestPrimaryEndLifePrepareSkipsWhenAtLowWater();
}

TEST_F(SpillEvictionTest, TestPrimaryEndLifePrepareUsesForegroundNeedSize)
{
    TestPrimaryEndLifePrepareUsesForegroundNeedSize();
}

TEST_F(SpillEvictionTest, TestDeleteAllCopyMetaResultCollectsPerKeyFailures)
{
    TestDeleteAllCopyMetaResultCollectsPerKeyFailures();
}

TEST_F(SpillEvictionTest, TestEvictObjectEndLifeKeepsLocalObjectWhenTaskAlreadyPending)
{
    TestEvictObjectEndLifeKeepsLocalObjectWhenTaskAlreadyPending();
}

TEST_F(SpillEvictionTest, TestPrimaryEndLifeFinishClearsPendingAndReaddsOnlyOnFailure)
{
    TestPrimaryEndLifeFinishClearsPendingAndReaddsOnlyOnFailure();
}
}  // namespace ut
}  // namespace datasystem
