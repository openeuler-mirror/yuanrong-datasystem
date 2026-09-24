/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Test EvictionManager.
 */
#include <fcntl.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>

#include "securec.h"

#include "../../../common/binmock/binmock.h"
#include "bench_helper.h"
#include "ut/common.h"
#include "ut/bthread_test_helper.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/flags/eviction_watermark.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/cluster/algorithm/topology_algorithm.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/object/buffer.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"
#include "eviction_manager_common.h"
#include "test_metadata_route.h"

using namespace datasystem::object_cache;
using namespace datasystem::worker;
using namespace datasystem::master;

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_size_limit);
DS_DECLARE_string(master_address);
DS_DECLARE_string(etcd_address);
DS_DECLARE_double(eviction_high_watermark_ratio);
DS_DECLARE_double(eviction_low_watermark_ratio);
DS_DECLARE_uint32(eviction_reserve_mem_threshold_mb);

namespace datasystem {
namespace ut {

class PrimaryEndLifeRoutingAlgorithm final : public cluster::IRoutingAlgorithm {
public:
    cluster::TopologyAlgorithmId GetId() const override
    {
        return "primary-end-life-test";
    }

    uint32_t Hash(std::string_view key) const noexcept override
    {
        return key.find("owner-b") == std::string_view::npos ? 1 : 2;
    }

    Status LocateOwner(const cluster::TopologySnapshot &snapshot, uint32_t token,
                       const cluster::Member *&owner) const override
    {
        const auto &activeMembers = snapshot.ActiveMembers();
        CHECK_FAIL_RETURN_STATUS(!activeMembers.empty(), K_NOT_FOUND, "No active metadata owner.");
        owner = token == 2 ? activeMembers.back() : activeMembers.front();
        return Status::OK();
    }

    Status LocateProspectiveOwner(const cluster::TopologySnapshot &snapshot, uint32_t token,
                                  const cluster::Member *&owner) const override
    {
        return LocateOwner(snapshot, token, owner);
    }
};

class FakeEvictionMasterApi final : public worker::WorkerLocalMasterOCApi {
public:
    using RemoveMetaHandler = std::function<Status(master::RemoveMetaReqPb &, master::RemoveMetaRspPb &)>;

    FakeEvictionMasterApi(const HostPort &localAddress, RemoveMetaHandler handler)
        : WorkerLocalMasterOCApi(nullptr, localAddress, nullptr), handler_(std::move(handler))
    {
    }

    ~FakeEvictionMasterApi() override = default;

    Status Init() override
    {
        return Status::OK();
    }

    Status RemoveMeta(master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) override
    {
        return handler_(request, response);
    }

private:
    RemoveMetaHandler handler_;
};

class FakeDeleteAllCopyMetaMasterApi final : public worker::WorkerLocalMasterOCApi {
public:
    using DeleteAllCopyMetaHandler =
        std::function<Status(master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &)>;

    FakeDeleteAllCopyMetaMasterApi(const HostPort &localAddress, DeleteAllCopyMetaHandler handler)
        : WorkerLocalMasterOCApi(nullptr, localAddress, nullptr), handler_(std::move(handler))
    {
    }

    ~FakeDeleteAllCopyMetaMasterApi() override = default;

    Status Init() override
    {
        return Status::OK();
    }

    Status DeleteAllCopyMeta(master::DeleteAllCopyMetaReqPb &request,
                             master::DeleteAllCopyMetaRspPb &response) override
    {
        return handler_(request, response);
    }

private:
    DeleteAllCopyMetaHandler handler_;
};

// Re-exposes the protected shutdown so tests can simulate a pool that rejects submissions.
class EvictionTestPool : public ThreadPool {
public:
    using ThreadPool::ThreadPool;
    using ThreadPool::ShutDown;
};

class EvictionManagerTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        objectTable_ = std::make_shared<ObjectTable>();
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(maxMemorySize);
        akSkManager_ = std::make_shared<AkSkManager>(0);
    }

    void InitEvictionManager(std::unique_ptr<WorkerOcEvictionManager> &manager,
                             std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &globalRefs)
    {
        manager = std::make_unique<WorkerOcEvictionManager>(
            objectTable_, HostPort("127.0.0.1", 31501), HostPort("127.0.0.1", 31500), GetTestMetadataRoute());
        globalRefs = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        DS_ASSERT_OK(manager->Init(globalRefs, akSkManager_));
    }

    void CheckPolicyLockBthreadProgress(bool routeLock)
    {
        std::unique_ptr<WorkerOcEvictionManager> manager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(manager, globalRefs);
        const std::string key = "missing-migration-key";
        if (routeLock) {
            manager->policyRouteMutex_.lock();
            ExpectBthreadProgressWhileBlocked(
                [&](size_t) { (void)manager->GetPolicyStateSnapshot(); },
                [&] { manager->policyRouteMutex_.unlock(); });
        } else {
            manager->GetPolicyMigrationLock(key).lock();
            ExpectBthreadProgressWhileBlocked(
                [&](size_t) { DS_EXPECT_OK(manager->MoveOnePolicyNode(key)); },
                [&] { manager->GetPolicyMigrationLock(key).unlock(); });
        }
    }

    void CreateObjectsBelowLowWaterMark(const std::shared_ptr<WorkerOcEvictionManager> &manager, size_t objectCount,
                                        uint64_t objectSize, const std::string &keyPrefix);

    size_t GetEvictionListCount(const std::shared_ptr<WorkerOcEvictionManager> &manager);

    bool AboveLowWaterMark(const std::shared_ptr<WorkerOcEvictionManager> &manager, uint64_t needSize)
    {
        return manager->IsAboveLowWaterMark(needSize, 0, CacheType::MEMORY);
    }

    void DrainEvictionPool(const std::shared_ptr<WorkerOcEvictionManager> &manager)
    {
        auto drained = manager->memEvictTaskThreadPool_->Submit([] {});
        ASSERT_EQ(drained.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        drained.get();
    }

    void CheckAllocationEviction(StatusCode allocationError, bool retryOnOOM, bool aboveLowWater, bool expectEviction)
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        constexpr uint64_t objectSize = 16 * 1024 * 1024;
        constexpr uint64_t requestSize = 1024 * 1024;
        const auto targetUsage = aboveLowWater ? manager->GetLowWaterMark(CacheType::MEMORY) : 0;
        for (size_t i = 0; i == 0 || allocator->GetTotalRealMemoryUsage() <= targetUsage; ++i) {
            auto key = "allocation-eviction-" + std::to_string(i);
            DS_ASSERT_OK(CreateObject(key, objectSize - GetMetaSize(objectSize),
                                      WriteMode::NONE_L2_CACHE, true, true));
            manager->Add(key);
        }
        ASSERT_FALSE(EvictWhenMemoryExceedThrehold("allocation-request", requestSize, manager, ServiceType::OBJECT,
                                                  CacheType::MEMORY));
        const auto usedBefore = GetAllocatedSize();
        ASSERT_EQ(manager->IsAboveLowWaterMark(requestSize, 0, CacheType::MEMORY), aboveLowWater);

        auto &timeout = GetRequestContext()->reqTimeoutDuration;
        const auto savedTimeout = timeout;
        Raii clearInjection([&timeout, savedTimeout] {
            (void)inject::Clear("worker.Allocator.AllocateMemory");
            timeout = savedTimeout;
        });
        if (allocationError != K_OK) {
            DS_ASSERT_OK(inject::Set("worker.Allocator.AllocateMemory",
                                     allocationError == K_OUT_OF_MEMORY ? "1*return(K_OUT_OF_MEMORY)"
                                                                        : "1*return(K_RUNTIME_ERROR)"));
        }
        GetRequestContext()->reqTimeoutDuration.Init(2000);
        ShmUnit unit;
        auto rc = AllocateMemoryForObject("allocation-request", requestSize, 0, false, manager, unit,
                                          CacheType::MEMORY, retryOnOOM);
        const bool expectSuccess = allocationError == K_OK || (allocationError == K_OUT_OF_MEMORY && retryOnOOM);
        ASSERT_EQ(rc.GetCode(), expectSuccess ? K_OK : allocationError) << rc.ToString();
        // A barrier on the single eviction executor observes completion without racing its asynchronous release.
        auto drained = manager->memEvictTaskThreadPool_->Submit([] {});
        ASSERT_EQ(drained.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        drained.get();
        const auto allocatedSize = expectSuccess ? requestSize : 0;
        if (expectEviction) {
            EXPECT_LT(GetAllocatedSize(), usedBefore + allocatedSize);
            EXPECT_FALSE(manager->IsAboveLowWaterMark(requestSize, 0, CacheType::MEMORY));
            EXPECT_GT(GetAllocatedSize(), allocatedSize);
        } else {
            EXPECT_EQ(GetAllocatedSize(), usedBefore + allocatedSize);
        }
    }

    void CheckOomRetryLimit(int64_t remainingTime, uint64_t expectedRetries, bool evictionRunning = false)
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        std::promise<void> releaseEviction;
        auto released = releaseEviction.get_future().share();
        Raii unblockEviction([&releaseEviction] { releaseEviction.set_value(); });
        if (evictionRunning) {
            manager->memEvictTaskThreadPool_->Execute([released] { released.wait(); });
            manager->Evict();
            ASSERT_FALSE(manager->isDone_.load());
        }
        auto &timeout = GetRequestContext()->reqTimeoutDuration;
        const auto savedTimeout = timeout;
        Raii clearInjection([&timeout, savedTimeout] {
            (void)inject::Clear("worker.Allocator.AllocateMemory");
            (void)inject::Clear("worker.AllocateMemory.sleepTime");
            timeout = savedTimeout;
        });
        // An uninitialized timeout returns its default duration without depending on wall-clock time or binary stubs.
        timeout = TimeoutDuration(remainingTime);
        DS_ASSERT_OK(inject::Set("worker.Allocator.AllocateMemory", "return(K_OUT_OF_MEMORY)"));
        DS_ASSERT_OK(inject::Set("worker.AllocateMemory.sleepTime", "call(0)"));
        ShmUnit unit;
        auto rc = AllocateMemoryForObject("oom-retry-limit", 1024, 0, false, manager, unit);
        EXPECT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);
        EXPECT_EQ(inject::GetExecuteCount("worker.Allocator.AllocateMemory"), expectedRetries + 1);
        EXPECT_EQ(inject::GetExecuteCount("worker.AllocateMemory.sleepTime"), expectedRetries);
        EXPECT_EQ(GetAllocatedSize(), 0U);
        if (evictionRunning) {
            EXPECT_FALSE(manager->isDone_.load());
        }
    }

    void AddThreeTrackedObjects(WorkerOcEvictionManager &manager,
                                const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &globalRefs)
    {
        DS_ASSERT_OK(CreateObject("id1", TEST_DATA_SIZE));
        manager.Add("id1");
        DS_ASSERT_OK(CreateObject("id2", TEST_DATA_SIZE, WriteMode::WRITE_THROUGH_L2_CACHE));
        manager.Add("id2");
        std::vector<std::string> keys{ "id3" };
        std::vector<std::string> failed;
        std::vector<std::string> firstIncrements;
        globalRefs->GIncreaseRef(ClientKey::Intern("client-id"), keys, failed, firstIncrements);
        DS_ASSERT_OK(CreateObject("id3", TEST_DATA_SIZE));
        manager.Add("id3");
    }

    void DeleteThreeObjects()
    {
        DS_ASSERT_OK(DeleteObject("id1"));
        DS_ASSERT_OK(DeleteObject("id2"));
        DS_ASSERT_OK(DeleteObject("id3"));
    }

    // Fixture method (not TEST_F body) so the friend declaration covers private-member access.
    void TestPrimaryEndLifeWatermarkSkipExemptsForceTasks()
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        const std::string key = "end-life-watermark-exempt";
        constexpr uint64_t needSize = 1024 * 1024;
        DS_ASSERT_OK(CreateObject(key, 16 * 1024 * 1024, WriteMode::NONE_L2_CACHE_EVICT, true, false));
        manager->Add(key);
        ASSERT_FALSE(AboveLowWaterMark(manager, needSize));

        std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> tasks;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> candidates;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> skipped;
        tasks.emplace_back(WorkerOcEvictionManager::PrimaryEndLifeTask{ key, 1, CacheType::MEMORY });
        DS_ASSERT_OK(manager->PreparePrimaryEndLifeCandidates(tasks, candidates, skipped));
        EXPECT_EQ(skipped.size(), 1U);
        EXPECT_TRUE(candidates.empty());

        tasks.front().forceEvict = true;
        skipped.clear();
        DS_ASSERT_OK(manager->PreparePrimaryEndLifeCandidates(tasks, candidates, skipped));
        EXPECT_TRUE(skipped.empty());
        ASSERT_EQ(candidates.size(), 1U);
        EXPECT_EQ(candidates.front().task.objectKey, key);
        EXPECT_TRUE(candidates.front().task.forceEvict);
        // The candidate takes over the entry write lock from PreparePrimaryEndLifeCandidates.
        candidates.front().entry->WUnlock();
    }

    // Fixture method (not TEST_F body) so the friend declaration covers private-member access.
    void TestEvictMergesPendingForceNeedIntoRunningTask()
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        constexpr uint64_t objectSize = 16 * 1024 * 1024;
        constexpr uint64_t needSize = 1024 * 1024;
        constexpr size_t objectCount = 4;
        CreateObjectsBelowLowWaterMark(manager, objectCount, objectSize, "force-pending-merge-");
        ASSERT_FALSE(AboveLowWaterMark(manager, needSize));
        const auto usedBefore = GetAllocatedSize();

        std::promise<void> releaseEviction;
        auto released = releaseEviction.get_future().share();
        std::atomic<bool> unblocked = false;
        auto unblock = [&releaseEviction, &unblocked]() {
            if (!unblocked.exchange(true)) {
                releaseEviction.set_value();
            }
        };
        Raii unblockGuard([unblock]() { unblock(); });
        manager->memEvictTaskThreadPool_->Execute([released] { released.wait(); });
        manager->Evict();
        ASSERT_FALSE(manager->isDone_.load());
        // A force request that loses the single-flight race must merge into the queued task instead of
        // being dropped.
        manager->Evict(needSize, CacheType::MEMORY, true);
        unblock();
        DrainEvictionPool(manager);
        EXPECT_LT(GetAllocatedSize(), usedBefore);
        EXPECT_LT(GetEvictionListCount(manager), objectCount);
    }

    WorkerOcEvictionManager::SpillTask MakeSpillTask(const std::string &key, uint64_t objectSize, const Status &rc,
                                                     bool released)
    {
        std::promise<WorkerOcEvictionManager::SpillResult> promise;
        promise.set_value(WorkerOcEvictionManager::SpillResult{ .rc = rc,
                                                                .elapsed = 0,
                                                                .location = nullptr,
                                                                .context = nullptr,
                                                                .released = released });
        auto trace = std::make_unique<WorkerOcEvictionManager::EvictionTrace>(key);
        trace->objectSize = objectSize;
        return WorkerOcEvictionManager::SpillTask{ promise.get_future(), std::move(trace), {} };
    }

    // Fixture method (not TEST_F body) so the friend declaration covers private-member access.
    void TestPendingForceNeedsAreHeldPerCacheType()
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        constexpr uint64_t bigNeed = 8 * 1024 * 1024;
        constexpr uint64_t smallNeed = 1024 * 1024;

        // Every cache type is its own water mark domain: a smaller need must not be swallowed by a
        // larger one of another type, or the producer would drop it before any task could serve it.
        manager->MergePendingForceNeed(bigNeed, CacheType::MEMORY);
        manager->MergePendingForceNeed(smallNeed, CacheType::DISK);
        ASSERT_EQ(manager->pendingForceNeeds_.size(), 2U);
        EXPECT_EQ(manager->pendingForceNeeds_.at(CacheType::MEMORY).needSize, bigNeed);
        EXPECT_EQ(manager->pendingForceNeeds_.at(CacheType::DISK).needSize, smallNeed);

        // Both survive a resubmit round each: the exit hook serves one per round and chains on.
        uint64_t taken = 0;
        CacheType takenType = CacheType::MEMORY;
        ASSERT_TRUE(manager->TakeAnyPendingForceNeed(taken, takenType));
        ASSERT_TRUE(manager->TakeAnyPendingForceNeed(taken, takenType));
        EXPECT_TRUE(manager->pendingForceNeeds_.empty());
        EXPECT_FALSE(manager->TakeAnyPendingForceNeed(taken, takenType));
    }

    // Fixture method (not TEST_F body) so the friend declaration covers private-member access.
    void TestExpiredPendingForceNeedIsDropped()
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        constexpr uint64_t needSize = 1024 * 1024;
        auto makeStale = [&manager]() {
            std::lock_guard<std::mutex> lock(manager->pendingForceMutex_);
            manager->pendingForceNeeds_[CacheType::MEMORY].arrivalMs = 0;
        };

        manager->MergePendingForceNeed(needSize, CacheType::MEMORY);
        makeStale();
        uint64_t need = 0;
        CacheType needType = CacheType::MEMORY;
        EXPECT_FALSE(manager->TakeAnyPendingForceNeed(need, needType));
        EXPECT_TRUE(manager->pendingForceNeeds_.empty());

        // A stale need must not be replayed either: upgrading an unrelated round below the water
        // mark for a memory state that no longer holds is exactly what the TTL prevents.
        manager->MergePendingForceNeed(needSize, CacheType::MEMORY);
        makeStale();
        WorkerOcEvictionManager::EvictionLoopState st;
        st.needSize = needSize;
        st.cacheType = CacheType::MEMORY;
        manager->MergePendingForceIntoTask(st);
        EXPECT_FALSE(st.forceEvict);
        EXPECT_TRUE(manager->pendingForceNeeds_.empty());
    }

    // Fixture method (not TEST_F body) so the friend declaration covers private-member access.
    void TestMergePendingForceIntoTaskOnlyConsumesOwnCacheType()
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        constexpr uint64_t needSize = 4 * 1024 * 1024;
        constexpr uint64_t smallNeed = 1024 * 1024;

        // A smaller need of the same type is covered by the running task: consumed, task turns force.
        manager->MergePendingForceNeed(smallNeed, CacheType::MEMORY);
        WorkerOcEvictionManager::EvictionLoopState sameType;
        sameType.needSize = needSize;
        sameType.cacheType = CacheType::MEMORY;
        manager->MergePendingForceIntoTask(sameType);
        EXPECT_TRUE(sameType.forceEvict);
        EXPECT_EQ(sameType.needSize, needSize);
        EXPECT_TRUE(manager->pendingForceNeeds_.empty());

        // A need of another type stays pending so the exit hook resubmits it as its own task.
        manager->MergePendingForceNeed(smallNeed, CacheType::DISK);
        WorkerOcEvictionManager::EvictionLoopState otherType;
        otherType.needSize = needSize;
        otherType.cacheType = CacheType::MEMORY;
        manager->MergePendingForceIntoTask(otherType);
        EXPECT_FALSE(otherType.forceEvict);
        EXPECT_EQ(otherType.needSize, needSize);
        ASSERT_EQ(manager->pendingForceNeeds_.count(CacheType::DISK), 1U);
        EXPECT_EQ(manager->pendingForceNeeds_.at(CacheType::DISK).needSize, smallNeed);

        // A bigger need of the same type raises the budget.
        manager->MergePendingForceNeed(2 * needSize, CacheType::MEMORY);
        WorkerOcEvictionManager::EvictionLoopState bigger;
        bigger.needSize = needSize;
        bigger.cacheType = CacheType::MEMORY;
        manager->MergePendingForceIntoTask(bigger);
        EXPECT_TRUE(bigger.forceEvict);
        EXPECT_EQ(bigger.needSize, 2 * needSize);
        EXPECT_EQ(bigger.cacheType, CacheType::MEMORY);
        EXPECT_EQ(manager->pendingForceNeeds_.count(CacheType::MEMORY), 0U);
    }

    // Fixture method (not TEST_F body) so the friend declaration covers private-member access.
    void TestReleaseSpillFuturesCountsOnlyReleasedFutures()
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        std::unordered_map<std::string, WorkerOcEvictionManager::SpillTask> spillTasks;
        WorkerOcEvictionManager::EvictionRetryList failedIds;
        spillTasks.emplace("released", MakeSpillTask("released", 40, Status::OK(), true));
        // OK without a local release is the shape of the "object gone or changed" no-op paths: the
        // future resolves but no byte returns to the arena.
        spillTasks.emplace("ok-not-released", MakeSpillTask("ok-not-released", 30, Status::OK(), false));
        spillTasks.emplace("failed", MakeSpillTask("failed", 20, Status(K_RUNTIME_ERROR, "spill failed"), false));

        auto reclaim = manager->ReleaseSpillFutures(spillTasks, failedIds, true);
        EXPECT_EQ(reclaim.resolvedSize, 90U);
        EXPECT_EQ(reclaim.freedSize, 40U);
        ASSERT_EQ(failedIds.size(), 1U);
    }

    // Fixture method (not TEST_F body) so the friend declaration covers private-member access.
    void TestResubmitPendingForceEvictionRunsTheTask()
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        constexpr uint64_t objectSize = 16 * 1024 * 1024;
        constexpr uint64_t needSize = 1024 * 1024;
        constexpr size_t objectCount = 4;
        CreateObjectsBelowLowWaterMark(manager, objectCount, objectSize, "force-resubmit-run-");
        ASSERT_FALSE(AboveLowWaterMark(manager, needSize));
        const auto usedBefore = GetAllocatedSize();

        // No task is running, so the exit-hook resubmit wins the single-flight slot and must really
        // run the forced round, not just hand the slot back.
        manager->MergePendingForceNeed(needSize, CacheType::MEMORY);
        ASSERT_TRUE(manager->isDone_.load());
        manager->ResubmitPendingForceEviction();
        DrainEvictionPool(manager);
        EXPECT_LT(GetAllocatedSize(), usedBefore);
        EXPECT_LT(GetEvictionListCount(manager), objectCount);
        EXPECT_TRUE(manager->isDone_.load());
        EXPECT_TRUE(manager->pendingForceNeeds_.empty());
    }

    // Fixture method (not TEST_F body) so the friend declaration covers private-member access.
    void TestResubmitPendingForceEvictionSurvivesPoolShutdown()
    {
        std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
        std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
        InitEvictionManager(uniqueManager, globalRefs);
        std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
        constexpr uint64_t needSize = 1024 * 1024;
        auto keepAlive = std::move(manager->memEvictTaskThreadPool_);
        auto *shutdownPool = new EvictionTestPool(1, 1, "ut_force_resubmit");
        shutdownPool->ShutDown();
        manager->memEvictTaskThreadPool_.reset(shutdownPool);
        manager->MergePendingForceNeed(needSize, CacheType::MEMORY);
        ASSERT_TRUE(manager->isDone_.load());
        // The resubmit runs in the eviction task exit hook: a submission rejected by the shutting
        // down pool must not escape it as an exception and must hand the slot back.
        manager->ResubmitPendingForceEviction();
        EXPECT_TRUE(manager->isDone_.load());
        EXPECT_TRUE(manager->pendingForceNeeds_.empty());
        // ThreadPool's destructor is not virtual; delete the derived pool through its own type.
        delete static_cast<EvictionTestPool *>(manager->memEvictTaskThreadPool_.release());
        manager->memEvictTaskThreadPool_ = std::move(keepAlive);
    }

    void InsertNoneL2EvictableMetadata(const std::string &objectKey)
    {
        auto object = std::make_unique<object_cache::ObjCacheShmUnit>();
        object->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        object->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE_EVICT);
        object->modeInfo.SetCacheType(CacheType::MEMORY);
        DS_ASSERT_OK(objectTable_->Insert(objectKey, std::move(object)));
    }

    Status PublishPrimaryEndLifeTopology(cluster::TopologySnapshotState &snapshots, uint64_t version,
                                         cluster::MemberState ownerAState)
    {
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = version;
        topology.members = {
            cluster::Member{ { std::string(16, 'a'), "127.0.0.1:31502" }, ownerAState, { 1 } },
            cluster::Member{ { std::string(16, 'b'), "127.0.0.1:31503" }, cluster::MemberState::ACTIVE, { 2 } }
        };
        if (ownerAState == cluster::MemberState::FAILED) {
            topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::FAILURE, version };
        }
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        RETURN_IF_NOT_OK(cluster::TopologySnapshot::Create(topology, static_cast<int64_t>(version),
                                                            std::string(64, static_cast<char>('a' + version)),
                                                            snapshot));
        cluster::SnapshotUpdateOutcome outcome;
        return snapshots.Publish(std::move(snapshot), outcome);
    }

    Status SubmitPrimaryEndLifeTaskForTest(WorkerOcEvictionManager &manager, const std::string &objectKey)
    {
        std::shared_ptr<SafeObjType> entry;
        RETURN_IF_NOT_OK(objectTable_->Get(objectKey, entry));
        WorkerOcEvictionManager::PrimaryEndLifeTask task{ objectKey, (*entry)->GetCreateTime(), CacheType::MEMORY,
                                                          maxMemorySize };
        task.queuedAtMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
        bool accepted = false;
        RETURN_IF_NOT_OK(manager.ReservePrimaryEndLifeTask(task, accepted));
        CHECK_FAIL_RETURN_STATUS(accepted, K_RUNTIME_ERROR, "Primary end-life test task was not accepted.");
        return manager.EnqueuePrimaryEndLifeTask(task);
    }

    void StartPrimaryEndLifeWorkersForTest(WorkerOcEvictionManager &manager)
    {
        manager.akSkManager_ = akSkManager_;
        DS_ASSERT_OK(manager.StartPrimaryEndLifeWorkers());
    }

    void MarkPrimaryEndLifeMetadataDeletedForTest(WorkerOcEvictionManager &manager, const std::string &objectKey)
    {
        std::shared_ptr<SafeObjType> entry;
        DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
        std::lock_guard<std::mutex> lock(manager.primaryEndLifeMutex_);
        manager.metaDeletedPrimaryEndLifeObjects_[objectKey] = (*entry)->GetCreateTime();
    }

    void VerifyPrimaryEndLifeRedirectResetsSourceRetryState()
    {
        const HostPort sourceOwner("127.0.0.1", 31502);
        const HostPort redirectOwner("127.0.0.1", 31503);
        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501), sourceOwner,
                                        GetTestMetadataRoute());
        WorkerOcEvictionManager::PrimaryEndLifeTask task{ "redirect-retry-reset", 1, CacheType::MEMORY };
        task.lastAttemptOwner = sourceOwner;
        task.lastAttemptTopologyVersion = 7;
        task.retryableFailureCount = 2;
        WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup redirectGroup;
        redirectGroup.masterAddress = redirectOwner;
        redirectGroup.topologyVersion = 8;
        redirectGroup.candidates.emplace_back(WorkerOcEvictionManager::PrimaryEndLifeCandidate{ task, nullptr });
        {
            std::lock_guard<std::mutex> lock(manager.primaryEndLifeMutex_);
            manager.primaryEndLifeStopping_ = false;
        }
        std::unordered_set<std::string> redirectKeys;
        manager.SchedulePrimaryEndLifeRedirects({ redirectGroup }, redirectKeys);

        WorkerOcEvictionManager::PrimaryEndLifeTask redirectedTask;
        {
            std::lock_guard<std::mutex> lock(manager.primaryEndLifeMutex_);
            ASSERT_EQ(manager.primaryEndLifeReadyQueue_.size(), 1U);
            redirectedTask = manager.primaryEndLifeReadyQueue_.front();
            manager.primaryEndLifeReadyQueue_.pop_front();
            manager.primaryEndLifeStopping_ = true;
        }
        EXPECT_TRUE(redirectedTask.lastAttemptOwner.Empty());
        EXPECT_EQ(redirectedTask.lastAttemptTopologyVersion, 0U);
        EXPECT_EQ(redirectedTask.retryableFailureCount, 0U);

        WorkerOcEvictionManager::PrimaryEndLifeOwnerBatch redirectBatch{ redirectOwner, 8, true,
                                                                          { redirectedTask } };
        WorkerOcEvictionManager::PrimaryEndLifeCandidate redirectCandidate{ redirectedTask, nullptr };
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> delayedTasks;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> readdTasks;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> forceDeleteCandidates;
        Status timeout(K_RPC_DEADLINE_EXCEEDED, "redirect timeout");
        manager.ClassifyPrimaryEndLifeRpcFailure(redirectBatch, redirectCandidate, timeout, delayedTasks, readdTasks,
                                                 forceDeleteCandidates);
        ASSERT_EQ(delayedTasks.size(), 1U);
        EXPECT_EQ(delayedTasks.front().retryableFailureCount, 0U);

        WorkerOcEvictionManager::PrimaryEndLifeOwnerBatch sourceBatch{ sourceOwner, 7, false,
                                                                        { delayedTasks.front() } };
        WorkerOcEvictionManager::PrimaryEndLifeCandidate sourceCandidate{ delayedTasks.front(), nullptr };
        delayedTasks.clear();
        manager.ClassifyPrimaryEndLifeRpcFailure(sourceBatch, sourceCandidate, timeout, delayedTasks, readdTasks,
                                                 forceDeleteCandidates);
        ASSERT_EQ(delayedTasks.size(), 1U);
        EXPECT_EQ(delayedTasks.front().retryableFailureCount, 1U);
        EXPECT_TRUE(forceDeleteCandidates.empty());
    }

    void VerifyPrimaryEndLifeRouteFailureReleasesPendingSlot()
    {
        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501),
                                        HostPort("127.0.0.1", 31502), GetTestMetadataRoute());
        WorkerOcEvictionManager::PrimaryEndLifeTask failedTask{ "route-failed", 1, CacheType::MEMORY };
        bool accepted = false;
        DS_ASSERT_OK(manager.ReservePrimaryEndLifeTask(failedTask, accepted));
        ASSERT_TRUE(accepted);
        worker::MetaOwnerRouteGroups grouped;
        grouped.failures.emplace(failedTask.objectKey, Status(K_NOT_FOUND, "route unavailable"));
        WorkerOcEvictionManager::PrimaryEndLifeTaskMap taskByKey{ { failedTask.objectKey, failedTask } };
        manager.ReaddPrimaryEndLifeRouteFailures(grouped, taskByKey);

        {
            std::lock_guard<std::mutex> lock(manager.primaryEndLifeMutex_);
            EXPECT_TRUE(manager.pendingPrimaryEndLifeObjects_.empty());
            EXPECT_TRUE(manager.delayedPrimaryEndLifeQueue_.empty());
        }
        EXPECT_TRUE(manager.memEvictionList_.Exist(failedTask.objectKey));

        WorkerOcEvictionManager::PrimaryEndLifeTask healthyTask{ "healthy-route", 2, CacheType::MEMORY };
        accepted = false;
        DS_ASSERT_OK(manager.ReservePrimaryEndLifeTask(healthyTask, accepted));
        EXPECT_TRUE(accepted);
        manager.ClearPrimaryEndLifePending(healthyTask);
    }

    void VerifyPrimaryEndLifeOwnerReleaseIsNoThrow()
    {
        static_assert(noexcept(std::declval<WorkerOcEvictionManager &>().ReleasePrimaryEndLifeOwner(nullptr)));
        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501),
                                        HostPort("127.0.0.1", 31502), GetTestMetadataRoute());
        WorkerOcEvictionManager::PrimaryEndLifeOwnerLane *lane = nullptr;
        {
            std::lock_guard<std::mutex> lock(manager.primaryEndLifeMutex_);
            auto [iter, inserted] = manager.primaryEndLifeOwnerLanes_.try_emplace(HostPort("127.0.0.1", 31502));
            ASSERT_TRUE(inserted);
            iter->second.inFlight = true;
            iter->second.waitingTasks.emplace_back(
                WorkerOcEvictionManager::PrimaryEndLifeTask{ "waiting-a", 1, CacheType::MEMORY });
            iter->second.waitingTasks.emplace_back(
                WorkerOcEvictionManager::PrimaryEndLifeTask{ "waiting-b", 2, CacheType::MEMORY });
            lane = &iter->second;
        }
        manager.ReleasePrimaryEndLifeOwner(lane);
        std::lock_guard<std::mutex> lock(manager.primaryEndLifeMutex_);
        EXPECT_TRUE(manager.primaryEndLifeOwnerLanes_.empty());
        EXPECT_EQ(manager.primaryEndLifeReadyQueue_.size(), 2U);
    }

    void VerifyPrimaryEndLifeAcquireRollbackBeforeCommit()
    {
        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501),
                                        HostPort("127.0.0.1", 31502), GetTestMetadataRoute());
        WorkerOcEvictionManager::PrimaryEndLifeOwnerBatchMap batches;
        const HostPort ownerA("127.0.0.1", 31502);
        const HostPort ownerB("127.0.0.1", 31503);
        batches.emplace(std::make_pair(ownerA, false),
                        WorkerOcEvictionManager::PrimaryEndLifeOwnerBatch{
                            ownerA, 1, false,
                            { WorkerOcEvictionManager::PrimaryEndLifeTask{ "owner-a", 1, CacheType::MEMORY } } });
        batches.emplace(std::make_pair(ownerB, false),
                        WorkerOcEvictionManager::PrimaryEndLifeOwnerBatch{
                            ownerB, 1, false,
                            { WorkerOcEvictionManager::PrimaryEndLifeTask{ "owner-b", 2, CacheType::MEMORY } } });
        DS_ASSERT_OK(inject::Set("WorkerOcEvictionManager.AcquirePrimaryOwnerBatch.beforeCommit", "call()"));
        EXPECT_THROW(manager.AcquirePrimaryOwnerBatch(batches), std::bad_alloc);
        DS_ASSERT_OK(inject::Clear("WorkerOcEvictionManager.AcquirePrimaryOwnerBatch.beforeCommit"));

        std::lock_guard<std::mutex> lock(manager.primaryEndLifeMutex_);
        EXPECT_TRUE(manager.primaryEndLifeOwnerLanes_.empty());
        EXPECT_TRUE(manager.primaryEndLifeReadyQueue_.empty());
    }

    bool WaitUntil(const std::function<bool()> &condition, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (condition()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return condition();
    }

    void TestEvictionRemoveMetaRequestRespectsRedirectPolicy()
    {
        WorkerOcEvictionManager::EvictDeletedObjects objectKeyVersions{ { "id1", 11 }, { "id2", 12 } };
        std::vector<std::string> objectKeys{ "id1", "id2" };
        auto req = WorkerOcEvictionManager::BuildEvictionRemoveMetaReq(
            objectKeys, objectKeyVersions, HostPort("127.0.0.1", 31501), true);

        EXPECT_TRUE(req.redirect());
        EXPECT_EQ(req.address(), "127.0.0.1:31501");
        EXPECT_EQ(req.cause(), master::RemoveMetaReqPb::EVICTION);
        EXPECT_EQ(req.version(), UINT64_MAX);
        ASSERT_EQ(req.id_with_version_size(), 2);
        EXPECT_EQ(req.id_with_version(0).id(), "id1");
        EXPECT_EQ(req.id_with_version(0).version(), 11U);
        EXPECT_EQ(req.id_with_version(1).id(), "id2");
        EXPECT_EQ(req.id_with_version(1).version(), 12U);

        auto forwardedReq = WorkerOcEvictionManager::BuildEvictionRemoveMetaReq(
            objectKeys, objectKeyVersions, HostPort("127.0.0.1", 31501), false);
        EXPECT_FALSE(forwardedReq.redirect());
    }

    void TestEvictionRedirectStopsAtForwardedTarget()
    {
        const HostPort sourceMaster("127.0.0.1", 31502);
        const HostPort targetMaster("127.0.0.1", 31503);
        const std::string objectKey = "redirected-eviction-key";
        size_t sourceCalls = 0;
        size_t targetCalls = 0;
        bool targetAllowsRedirect = true;

        auto sourceApi = std::make_shared<FakeEvictionMasterApi>(
            sourceMaster, [&](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
                ++sourceCalls;
                response.Clear();
                if (sourceCalls > 1) {
                    return Status(K_RUNTIME_ERROR, "redirect loop reached source master again");
                }
                EXPECT_TRUE(request.redirect());
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(targetMaster.ToString());
                redirectInfo->add_change_meta_ids(objectKey);
                return Status::OK();
            });
        auto targetApi = std::make_shared<FakeEvictionMasterApi>(
            targetMaster, [&](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
                ++targetCalls;
                targetAllowsRedirect = request.redirect();
                response.Clear();
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(sourceMaster.ToString());
                redirectInfo->add_change_meta_ids(objectKey);
                return Status::OK();
            });

        BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                       (testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke(
                [&](const HostPort &masterAddress, const HostPort &, std::shared_ptr<AkSkManager>,
                    master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> {
                    if (masterAddress.ToString() == targetMaster.ToString()) {
                        return targetApi;
                    }
                    return sourceApi;
                }));

        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501), sourceMaster,
                                        GetTestMetadataRoute());
        WorkerOcEvictionManager::EvictDeletedObjects objectKeyVersions{ { objectKey, 1 } };
        WorkerOcEvictionManager::EvictDeletedObjects failedObjects;
        Status lastRc;
        manager.RemoveEvictionMetaGroup(sourceMaster, { objectKey }, objectKeyVersions, failedObjects, lastRc);
        RELEASE_STUBS

        EXPECT_EQ(sourceCalls, 1U);
        EXPECT_EQ(targetCalls, 1U);
        EXPECT_FALSE(targetAllowsRedirect);
        ASSERT_EQ(failedObjects.size(), 1U);
        EXPECT_EQ(failedObjects.at(objectKey), 1U);
        EXPECT_EQ(lastRc.GetCode(), K_TRY_AGAIN);
    }

    void TestPrimaryEndLifeRedirectForwardsOnceAndPreservesRequest()
    {
        const HostPort sourceMaster("127.0.0.1", 31502);
        const HostPort targetMaster("127.0.0.1", 31503);
        const HostPort workerAddress("127.0.0.1", 31501);
        const std::string objectKey = "primary-end-life-redirect";
        constexpr uint64_t objectVersion = 101;
        size_t sourceCalls = 0;
        size_t targetCalls = 0;

        auto sourceApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            sourceMaster, [&](master::DeleteAllCopyMetaReqPb &request, master::DeleteAllCopyMetaRspPb &response) {
                ++sourceCalls;
                EXPECT_TRUE(request.redirect());
                EXPECT_TRUE(request.async_delete());
                EXPECT_EQ(request.address(), workerAddress.ToString());
                EXPECT_EQ(request.ids_with_version_size(), 1);
                if (request.ids_with_version_size() == 1) {
                    EXPECT_EQ(request.ids_with_version(0).id(), objectKey);
                    EXPECT_EQ(request.ids_with_version(0).version(), objectVersion);
                }
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(targetMaster.ToString());
                redirectInfo->add_change_meta_ids(objectKey);
                redirectInfo->set_topology_version(7);
                response.mutable_last_rc()->set_error_code(K_RPC_DEADLINE_EXCEEDED);
                response.mutable_last_rc()->set_error_msg("source owner changed before completion");
                return Status::OK();
            });
        auto targetApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            targetMaster, [&](master::DeleteAllCopyMetaReqPb &request, master::DeleteAllCopyMetaRspPb &) {
                ++targetCalls;
                EXPECT_FALSE(request.redirect());
                EXPECT_TRUE(request.async_delete());
                EXPECT_EQ(request.address(), workerAddress.ToString());
                EXPECT_EQ(request.ids_with_version_size(), 1);
                if (request.ids_with_version_size() == 1) {
                    EXPECT_EQ(request.ids_with_version(0).id(), objectKey);
                    EXPECT_EQ(request.ids_with_version(0).version(), objectVersion);
                }
                return Status::OK();
            });

        BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                       (testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke(
                [&](const HostPort &masterAddress, const HostPort &, std::shared_ptr<AkSkManager>,
                    master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> {
                    return masterAddress.ToString() == targetMaster.ToString() ? targetApi : sourceApi;
                }));

        WorkerOcEvictionManager manager(objectTable_, workerAddress, sourceMaster, GetTestMetadataRoute());
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> candidates{
            { { objectKey, objectVersion, CacheType::MEMORY }, nullptr }
        };
        std::unordered_set<std::string> failedKeys;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup> redirectGroups;
        Status rc = manager.DeletePrimaryEndLifeMetadata(sourceMaster, candidates, true, 0, 1000, failedKeys,
                                                         redirectGroups);
        RELEASE_STUBS

        DS_ASSERT_OK(rc);
        EXPECT_EQ(sourceCalls, 1U);
        EXPECT_EQ(targetCalls, 0U);
        EXPECT_TRUE(failedKeys.empty());
        ASSERT_EQ(redirectGroups.size(), 1U);
        EXPECT_EQ(redirectGroups.front().masterAddress, targetMaster);
        ASSERT_EQ(redirectGroups.front().candidates.size(), 1U);
        EXPECT_EQ(redirectGroups.front().candidates.front().task.objectKey, objectKey);
    }

    void TestPrimaryEndLifeSecondRedirectStopsAtTarget()
    {
        const HostPort sourceMaster("127.0.0.1", 31502);
        const HostPort targetMaster("127.0.0.1", 31503);
        const HostPort workerAddress("127.0.0.1", 31501);
        const std::string objectKey = "primary-end-life-second-redirect";
        size_t sourceCalls = 0;
        size_t targetCalls = 0;

        auto sourceApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            sourceMaster, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &response) {
                ++sourceCalls;
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(targetMaster.ToString());
                redirectInfo->add_change_meta_ids(objectKey);
                return Status::OK();
            });
        auto targetApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            targetMaster, [&](master::DeleteAllCopyMetaReqPb &request, master::DeleteAllCopyMetaRspPb &response) {
                ++targetCalls;
                EXPECT_FALSE(request.redirect());
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(sourceMaster.ToString());
                redirectInfo->add_change_meta_ids(objectKey);
                return Status::OK();
            });

        BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                       (testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke(
                [&](const HostPort &masterAddress, const HostPort &, std::shared_ptr<AkSkManager>,
                    master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> {
                    return masterAddress.ToString() == targetMaster.ToString() ? targetApi : sourceApi;
                }));

        WorkerOcEvictionManager manager(objectTable_, workerAddress, sourceMaster, GetTestMetadataRoute());
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> candidates{
            { { objectKey, 102, CacheType::MEMORY }, nullptr }
        };
        std::unordered_set<std::string> failedKeys;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup> redirectGroups;
        Status rc = manager.DeletePrimaryEndLifeMetadata(sourceMaster, candidates, true, 0, 1000, failedKeys,
                                                         redirectGroups);
        ASSERT_TRUE(rc.IsOk());
        ASSERT_EQ(redirectGroups.size(), 1U);
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup> unexpectedRedirects;
        rc = manager.DeletePrimaryEndLifeMetadata(targetMaster, redirectGroups.front().candidates, false, 0, 1000,
                                                  failedKeys, unexpectedRedirects);
        RELEASE_STUBS

        DS_ASSERT_OK(rc);
        EXPECT_EQ(sourceCalls, 1U);
        EXPECT_EQ(targetCalls, 1U);
        EXPECT_EQ(failedKeys, std::unordered_set<std::string>{ objectKey });
        EXPECT_TRUE(unexpectedRedirects.empty());
    }

    void TestPrimaryEndLifeMixedRedirectResultOnlyReaddsFailedKeys()
    {
        const HostPort sourceMaster("127.0.0.1", 31502);
        const HostPort targetMaster("127.0.0.1", 31503);
        const HostPort workerAddress("127.0.0.1", 31501);
        const std::string acceptedKey = "primary-end-life-accepted";
        const std::string sourceFailedKey = "primary-end-life-source-failed";
        const std::string targetAcceptedKey = "primary-end-life-target-accepted";
        const std::string targetFailedKey = "primary-end-life-target-failed";
        size_t sourceCalls = 0;
        size_t targetCalls = 0;

        auto sourceApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            sourceMaster, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &response) {
                ++sourceCalls;
                response.add_failed_object_keys(sourceFailedKey);
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(targetMaster.ToString());
                redirectInfo->add_change_meta_ids(targetAcceptedKey);
                redirectInfo->add_change_meta_ids(targetFailedKey);
                response.mutable_last_rc()->set_error_code(K_RUNTIME_ERROR);
                response.mutable_last_rc()->set_error_msg("partial source failure");
                return Status::OK();
            });
        auto targetApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            targetMaster, [&](master::DeleteAllCopyMetaReqPb &request, master::DeleteAllCopyMetaRspPb &response) {
                ++targetCalls;
                EXPECT_FALSE(request.redirect());
                EXPECT_EQ(request.ids_with_version_size(), 2);
                if (request.ids_with_version_size() == 2) {
                    EXPECT_EQ(request.ids_with_version(0).id(), targetAcceptedKey);
                    EXPECT_EQ(request.ids_with_version(0).version(), 103U);
                    EXPECT_EQ(request.ids_with_version(1).id(), targetFailedKey);
                    EXPECT_EQ(request.ids_with_version(1).version(), 104U);
                }
                response.add_failed_object_keys(targetFailedKey);
                response.mutable_last_rc()->set_error_code(K_RUNTIME_ERROR);
                response.mutable_last_rc()->set_error_msg("partial target failure");
                return Status::OK();
            });

        BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                       (testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke(
                [&](const HostPort &masterAddress, const HostPort &, std::shared_ptr<AkSkManager>,
                    master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> {
                    return masterAddress.ToString() == targetMaster.ToString() ? targetApi : sourceApi;
                }));

        WorkerOcEvictionManager manager(objectTable_, workerAddress, sourceMaster, GetTestMetadataRoute());
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> candidates{
            { { acceptedKey, 101, CacheType::MEMORY }, nullptr },
            { { sourceFailedKey, 102, CacheType::MEMORY }, nullptr },
            { { targetAcceptedKey, 103, CacheType::MEMORY }, nullptr },
            { { targetFailedKey, 104, CacheType::MEMORY }, nullptr }
        };
        std::unordered_set<std::string> failedKeys;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup> redirectGroups;
        Status rc = manager.DeletePrimaryEndLifeMetadata(sourceMaster, candidates, true, 0, 1000, failedKeys,
                                                         redirectGroups);
        ASSERT_TRUE(rc.IsOk());
        ASSERT_EQ(redirectGroups.size(), 1U);
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup> unexpectedRedirects;
        std::unordered_set<std::string> targetFailedKeys;
        rc = manager.DeletePrimaryEndLifeMetadata(targetMaster, redirectGroups.front().candidates, false, 0, 1000,
                                                  targetFailedKeys, unexpectedRedirects);
        failedKeys.insert(targetFailedKeys.begin(), targetFailedKeys.end());
        RELEASE_STUBS

        DS_ASSERT_OK(rc);
        EXPECT_EQ(sourceCalls, 1U);
        EXPECT_EQ(targetCalls, 1U);
        EXPECT_EQ(failedKeys, (std::unordered_set<std::string>{ sourceFailedKey, targetFailedKey }));
    }

    void TestPrimaryEndLifeRedirectTargetTimeoutDoesNotForceDelete()
    {
        const HostPort sourceMaster("127.0.0.1", 31502);
        const HostPort targetMaster("127.0.0.1", 31503);
        const HostPort workerAddress("127.0.0.1", 31501);
        const std::string objectKey = "primary-end-life-target-timeout";
        size_t sourceCalls = 0;
        size_t targetCalls = 0;

        auto sourceApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            sourceMaster, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &response) {
                ++sourceCalls;
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(targetMaster.ToString());
                redirectInfo->add_change_meta_ids(objectKey);
                return Status::OK();
            });
        auto targetApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            targetMaster, [&](master::DeleteAllCopyMetaReqPb &request, master::DeleteAllCopyMetaRspPb &) {
                ++targetCalls;
                EXPECT_FALSE(request.redirect());
                return Status(K_RPC_DEADLINE_EXCEEDED, "redirect target timeout");
            });

        BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                       (testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke(
                [&](const HostPort &masterAddress, const HostPort &, std::shared_ptr<AkSkManager>,
                    master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> {
                    return masterAddress.ToString() == targetMaster.ToString() ? targetApi : sourceApi;
                }));

        WorkerOcEvictionManager manager(objectTable_, workerAddress, sourceMaster, GetTestMetadataRoute());
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> candidates{
            { { objectKey, 105, CacheType::MEMORY }, nullptr }
        };
        std::unordered_set<std::string> failedKeys;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup> redirectGroups;
        Status rc = manager.DeletePrimaryEndLifeMetadata(sourceMaster, candidates, true, 0, 1000, failedKeys,
                                                         redirectGroups);
        ASSERT_TRUE(rc.IsOk());
        ASSERT_EQ(redirectGroups.size(), 1U);
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup> unexpectedRedirects;
        rc = manager.DeletePrimaryEndLifeMetadata(targetMaster, redirectGroups.front().candidates, false, 0, 1000,
                                                  failedKeys, unexpectedRedirects);
        RELEASE_STUBS

        EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
        EXPECT_EQ(sourceCalls, 1U);
        EXPECT_EQ(targetCalls, 1U);
        EXPECT_TRUE(failedKeys.empty());
    }

    void TestPrimaryEndLifeMalformedRedirectFailsClosed()
    {
        const HostPort sourceMaster("127.0.0.1", 31502);
        const HostPort workerAddress("127.0.0.1", 31501);
        const std::string objectKey = "primary-end-life-malformed-redirect";
        size_t sourceCalls = 0;
        auto sourceApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            sourceMaster, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &response) {
                ++sourceCalls;
                response.add_info()->set_redirect_meta_address("127.0.0.1:31503");
                return Status::OK();
            });

        BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                       (testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Return(sourceApi));

        WorkerOcEvictionManager manager(objectTable_, workerAddress, sourceMaster, GetTestMetadataRoute());
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> candidates{
            { { objectKey, 106, CacheType::MEMORY }, nullptr }
        };
        std::unordered_set<std::string> failedKeys;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup> redirectGroups;
        Status rc = manager.DeletePrimaryEndLifeMetadata(sourceMaster, candidates, true, 0, 1000, failedKeys,
                                                         redirectGroups);
        RELEASE_STUBS

        DS_ASSERT_OK(rc);
        EXPECT_EQ(sourceCalls, 1U);
        EXPECT_EQ(failedKeys, std::unordered_set<std::string>{ objectKey });
    }

    void TestPrimaryEndLifeSourceTimeoutUsesOneRpcPerCall()
    {
        const HostPort sourceMaster("127.0.0.1", 31502);
        const HostPort workerAddress("127.0.0.1", 31501);
        const std::string objectKey = "primary-end-life-source-timeout";
        size_t sourceCalls = 0;
        auto sourceApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            sourceMaster, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &) {
                ++sourceCalls;
                return Status(K_RPC_DEADLINE_EXCEEDED, "source timeout");
            });

        BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                       (testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Return(sourceApi));

        WorkerOcEvictionManager manager(objectTable_, workerAddress, sourceMaster, GetTestMetadataRoute());
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeCandidate> candidates{
            { { objectKey, 106, CacheType::MEMORY }, nullptr }
        };
        std::unordered_set<std::string> failedKeys;
        std::vector<WorkerOcEvictionManager::PrimaryEndLifeRedirectGroup> redirectGroups;
        Status rc = manager.DeletePrimaryEndLifeMetadata(sourceMaster, candidates, true, 0, 1000, failedKeys,
                                                         redirectGroups);
        RELEASE_STUBS

        EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
        EXPECT_EQ(sourceCalls, 1U);
        EXPECT_TRUE(failedKeys.empty());
    }

    void TestNoneL2FallbackRedirectForwardsOnce()
    {
        const HostPort sourceMaster("127.0.0.1", 31500);
        const HostPort targetMaster("127.0.0.1", 31503);
        const std::string objectKey = "none-l2-fallback-redirect";
        size_t sourceCalls = 0;
        size_t targetCalls = 0;
        auto sourceApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            sourceMaster, [&](master::DeleteAllCopyMetaReqPb &request, master::DeleteAllCopyMetaRspPb &response) {
                ++sourceCalls;
                EXPECT_TRUE(request.redirect());
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(targetMaster.ToString());
                redirectInfo->add_change_meta_ids(objectKey);
                return Status::OK();
            });
        auto targetApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            targetMaster, [&](master::DeleteAllCopyMetaReqPb &request, master::DeleteAllCopyMetaRspPb &) {
                ++targetCalls;
                EXPECT_FALSE(request.redirect());
                EXPECT_EQ(request.object_keys_size(), 1);
                if (request.object_keys_size() == 1) {
                    EXPECT_EQ(request.object_keys(0), objectKey);
                }
                return Status::OK();
            });

        BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                       (testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke(
                [&](const HostPort &masterAddress, const HostPort &, std::shared_ptr<AkSkManager>,
                    master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> {
                    return masterAddress.ToString() == targetMaster.ToString() ? targetApi : sourceApi;
                }));

        InsertNoneL2EvictableMetadata(objectKey);
        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501), sourceMaster,
                                        GetTestMetadataRoute());
        std::shared_ptr<SafeObjType> entry;
        DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
        DS_ASSERT_OK(entry->WLock());
        Status rc = manager.DeleteNoneL2CacheEvictableObject(ObjectKV(objectKey, *entry));
        entry->WUnlock();
        RELEASE_STUBS

        DS_ASSERT_OK(rc);
        EXPECT_EQ(sourceCalls, 1U);
        EXPECT_EQ(targetCalls, 1U);
        EXPECT_FALSE(objectTable_->Contains(objectKey));
    }

    void TestNoneL2FallbackSecondRedirectKeepsLocalObject()
    {
        const HostPort sourceMaster("127.0.0.1", 31500);
        const HostPort targetMaster("127.0.0.1", 31503);
        const std::string objectKey = "none-l2-fallback-second-redirect";
        size_t sourceCalls = 0;
        size_t targetCalls = 0;
        auto sourceApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            sourceMaster, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &response) {
                ++sourceCalls;
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(targetMaster.ToString());
                redirectInfo->add_change_meta_ids(objectKey);
                return Status::OK();
            });
        auto targetApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
            targetMaster, [&](master::DeleteAllCopyMetaReqPb &request, master::DeleteAllCopyMetaRspPb &response) {
                ++targetCalls;
                EXPECT_FALSE(request.redirect());
                auto *redirectInfo = response.add_info();
                redirectInfo->set_redirect_meta_address(sourceMaster.ToString());
                redirectInfo->add_change_meta_ids(objectKey);
                return Status::OK();
            });

        BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                       (testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke(
                [&](const HostPort &masterAddress, const HostPort &, std::shared_ptr<AkSkManager>,
                    master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> {
                    return masterAddress.ToString() == targetMaster.ToString() ? targetApi : sourceApi;
                }));

        InsertNoneL2EvictableMetadata(objectKey);
        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501), sourceMaster,
                                        GetTestMetadataRoute());
        std::shared_ptr<SafeObjType> entry;
        DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
        DS_ASSERT_OK(entry->WLock());
        Status rc = manager.DeleteNoneL2CacheEvictableObject(ObjectKV(objectKey, *entry));
        entry->WUnlock();
        RELEASE_STUBS

        EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
        EXPECT_EQ(sourceCalls, 1U);
        EXPECT_EQ(targetCalls, 1U);
        EXPECT_TRUE(objectTable_->Contains(objectKey));
        DS_ASSERT_OK(DeleteObject(objectKey));
    }

    static constexpr uint64_t TEST_DATA_SIZE = 10 * 1024 * 1024;
    std::shared_ptr<AkSkManager> akSkManager_;
};

TEST_F(EvictionManagerTest, PolicyRouteContentionPreservesBthreadProgress)
{
    CheckPolicyLockBthreadProgress(true);
}

TEST_F(EvictionManagerTest, PolicyMigrationContentionPreservesBthreadProgress)
{
    CheckPolicyLockBthreadProgress(false);
}

TEST_F(EvictionManagerTest, TestAllocator)
{
    ASSERT_EQ(GetMaxMemorySize(), maxMemorySize);
    ASSERT_EQ(GetAllocatedSize(), size_t(0));

    uint64_t dataSize = 1024 * 1024;
    auto metaSize = GetMetaSize(dataSize);
    for (int i = 1; i <= 10; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize));
        DS_EXPECT_NOT_OK(CreateObject(objectKey, dataSize));
        ASSERT_EQ(GetAllocatedSize(), i * (dataSize + metaSize));
    }

    for (int i = 10; i >= 1; i--) {
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(DeleteObject(objectKey));
        DS_EXPECT_NOT_OK(DeleteObject(objectKey));
        ASSERT_EQ(GetAllocatedSize(), (i - 1) * (dataSize + metaSize));
    }
}

TEST_F(EvictionManagerTest, TestEvictionList)
{
    object_cache::EvictionList evictionList;
    ASSERT_EQ(evictionList.Size(), size_t(0));
    for (int i = 1; i <= 4; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        uint8_t counter = i;
        evictionList.Add(objectKey, counter);
    }
    ASSERT_EQ(evictionList.Size(), size_t(4));

    for (int i = 1; i <= 4; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        EvictionList::Node node;
        DS_EXPECT_OK(evictionList.GetObjectInfo(objectKey, node));
        ASSERT_EQ(node.curCounter, i);
        ASSERT_EQ(node.maxCounter, i);
    }

    EvictionList::Node node;
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_1" && node.curCounter == 1 && node.maxCounter == 1);

    (void)evictionList.Erase("key_1");
    ASSERT_EQ(evictionList.Size(), size_t(3));
    DS_EXPECT_NOT_OK(evictionList.GetObjectInfo("key_1", node));
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_2" && node.curCounter == 2 && node.maxCounter == 2);

    (void)evictionList.Erase("key_3");
    ASSERT_EQ(evictionList.Size(), size_t(2));
    DS_EXPECT_NOT_OK(evictionList.GetObjectInfo("key_3", node));
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_2" && node.curCounter == 2 && node.maxCounter == 2);

    (void)evictionList.Erase("key_2");
    ASSERT_EQ(evictionList.Size(), size_t(1));
    DS_EXPECT_NOT_OK(evictionList.GetObjectInfo("key_2", node));
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_4" && node.curCounter == 4 && node.maxCounter == 4);

    (void)evictionList.Erase("key_4");
    DS_EXPECT_NOT_OK(evictionList.GetObjectInfo("key_4", node));
    ASSERT_EQ(evictionList.Size(), size_t(0));

    // Add again
    evictionList.Add("key_4", 4);
    ASSERT_EQ(evictionList.Size(), size_t(1));
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_4" && node.curCounter == 4 && node.maxCounter == 4);
}

TEST_F(EvictionManagerTest, TestEvictionManagerInit)
{
    std::shared_ptr<ObjectTable> &objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, HostPort("127.0.0.1", 31501),
                                                          HostPort("127.0.0.1", 31500), GetTestMetadataRoute());
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));
    std::vector<EvictionList::Node> objsInList;
    EvictionList::Node oldest;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    ASSERT_EQ(objsInList.size(), size_t(0));
}

TEST_F(EvictionManagerTest, AllocationOomBelowHighWatermarkTriggersEviction)
{
    CheckAllocationEviction(K_OUT_OF_MEMORY, true, true, true);
}

TEST_F(EvictionManagerTest, AllocationOomRespectsLowWatermark)
{
    CheckAllocationEviction(K_OUT_OF_MEMORY, true, false, false);
}

TEST_F(EvictionManagerTest, AllocationOomWithoutRetryDoesNotEvict)
{
    CheckAllocationEviction(K_OUT_OF_MEMORY, false, true, false);
}

TEST_F(EvictionManagerTest, AllocationNonOomErrorDoesNotEvict)
{
    CheckAllocationEviction(K_RUNTIME_ERROR, true, true, false);
}

TEST_F(EvictionManagerTest, AllocationSuccessBelowHighWatermarkDoesNotEvict)
{
    CheckAllocationEviction(K_OK, true, true, false);
}

TEST_F(EvictionManagerTest, AllocationPersistentOomHasBoundedRetries)
{
    CheckOomRetryLimit(2000, 13);
}

TEST_F(EvictionManagerTest, AllocationOomRetriesWithTwentyMillisecondsLeft)
{
    CheckOomRetryLimit(20, 13);
}

TEST_F(EvictionManagerTest, AllocationOomRetriesWhileEvictionIsRunning)
{
    CheckOomRetryLimit(2000, 13, true);
}

TEST_F(EvictionManagerTest, AllocationOomReservesReplyTime)
{
    CheckOomRetryLimit(5, 0);
}

void EvictionManagerTest::CreateObjectsBelowLowWaterMark(
    const std::shared_ptr<WorkerOcEvictionManager> &manager, size_t objectCount, uint64_t objectSize,
    const std::string &keyPrefix)
{
    for (size_t i = 0; i < objectCount; ++i) {
        auto key = keyPrefix + std::to_string(i);
        DS_ASSERT_OK(CreateObject(key, objectSize - GetMetaSize(objectSize), WriteMode::NONE_L2_CACHE, true, true));
        manager->Add(key);
    }
}

size_t EvictionManagerTest::GetEvictionListCount(const std::shared_ptr<WorkerOcEvictionManager> &manager)
{
    std::vector<EvictionList::Node> nodes;
    EvictionList::Node oldest;
    DS_EXPECT_OK(manager->GetAllObjectsInfo(nodes, oldest));
    return nodes.size();
}

// Extent-unavailable OOM (issue #1211): usage stays below the low water mark while no contiguous
// extent can serve the request. Forced eviction must still run in that state, but stay bounded.
TEST_F(EvictionManagerTest, EvictionTaskForceEvictsBelowLowWaterMark)
{
    std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
    InitEvictionManager(uniqueManager, globalRefs);
    std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
    constexpr uint64_t objectSize = 16 * 1024 * 1024;
    constexpr uint64_t needSize = 1024 * 1024;
    constexpr size_t objectCount = 13;
    CreateObjectsBelowLowWaterMark(manager, objectCount, objectSize, "force-evict-sync-");
    ASSERT_FALSE(AboveLowWaterMark(manager, needSize));
    const auto usedBefore = GetAllocatedSize();

    manager->EvictionTaskForTest(needSize, CacheType::MEMORY, false);
    EXPECT_EQ(GetAllocatedSize(), usedBefore);
    EXPECT_EQ(GetEvictionListCount(manager), objectCount);

    manager->EvictionTaskForTest(needSize, CacheType::MEMORY, true);
    EXPECT_LT(GetAllocatedSize(), usedBefore);
    EXPECT_LT(GetEvictionListCount(manager), objectCount);
    // Force eviction stops once needSize is covered by one candidate, it must not drain the cache.
    EXPECT_GT(GetEvictionListCount(manager), objectCount / 2);
}

TEST_F(EvictionManagerTest, EvictAsyncPropagatesForceBelowLowWaterMark)
{
    std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
    InitEvictionManager(uniqueManager, globalRefs);
    std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
    constexpr uint64_t objectSize = 16 * 1024 * 1024;
    constexpr uint64_t needSize = 1024 * 1024;
    constexpr size_t objectCount = 13;
    CreateObjectsBelowLowWaterMark(manager, objectCount, objectSize, "force-evict-async-");
    ASSERT_FALSE(AboveLowWaterMark(manager, needSize));
    const auto usedBefore = GetAllocatedSize();

    manager->Evict(needSize, CacheType::MEMORY, true);
    // A barrier on the single eviction executor observes completion without racing its asynchronous release.
    DrainEvictionPool(manager);
    EXPECT_LT(GetAllocatedSize(), usedBefore);
    EXPECT_LT(GetEvictionListCount(manager), objectCount);
}

TEST_F(EvictionManagerTest, ForceEvictionEndLifeIsNotCountedAndExemptedTasksFreeMemory)
{
    size_t deleteCalls = 0;
    auto fakeApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
        HostPort("127.0.0.1", 31500),
        [&deleteCalls](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &) {
            ++deleteCalls;
            return Status::OK();
        });
    BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi, (testing::_, testing::_, testing::_, testing::_))
        .WillRepeatedly(testing::Invoke(
            [&](const HostPort &, const HostPort &, std::shared_ptr<AkSkManager>,
                master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> { return fakeApi; }));
    Raii releaseStubs([]() { RELEASE_STUBS });

    std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
    InitEvictionManager(uniqueManager, globalRefs);
    std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
    constexpr uint64_t objectSize = 16 * 1024 * 1024;
    constexpr uint64_t needSize = 1024 * 1024;
    constexpr size_t objectCount = 4;
    for (size_t i = 0; i < objectCount; ++i) {
        auto key = "force-end-life-" + std::to_string(i);
        DS_ASSERT_OK(
            CreateObject(key, objectSize - GetMetaSize(objectSize), WriteMode::NONE_L2_CACHE_EVICT, true, false));
        manager->Add(key);
    }
    ASSERT_FALSE(AboveLowWaterMark(manager, needSize));
    const auto usedBefore = GetAllocatedSize();

    manager->EvictionTaskForTest(needSize, CacheType::MEMORY, true);
    // END_LIFE acceptance is a queued promise, not a synchronous release: the confirmed budget must
    // stay untouched and nothing may be freed yet.
    EXPECT_EQ(GetAllocatedSize(), usedBefore);
    // The accepted bytes count as a promise against the obligation, so a 1MiB need stops after the
    // first 16MiB acceptance instead of handing the whole list to the end-life lane.
    EXPECT_EQ(GetEvictionListCount(manager), objectCount - 1);

    const auto waitDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (GetAllocatedSize() >= usedBefore && std::chrono::steady_clock::now() < waitDeadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    // The exempted tasks really release memory through the drain lane even below the water mark.
    EXPECT_LT(GetAllocatedSize(), usedBefore);
    EXPECT_GT(deleteCalls, 0U);
}

TEST_F(EvictionManagerTest, PrimaryEndLifeWatermarkSkipExemptsForceTasks)
{
    TestPrimaryEndLifeWatermarkSkipExemptsForceTasks();
}

TEST_F(EvictionManagerTest, ForceEvictionDoesNotCountFailedSpill)
{
    std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
    InitEvictionManager(uniqueManager, globalRefs);
    std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
    constexpr uint64_t objectSize = 16 * 1024 * 1024;
    constexpr uint64_t needSize = 1024 * 1024;
    constexpr size_t objectCount = 4;
    CreateObjectsBelowLowWaterMark(manager, objectCount, objectSize, "force-spill-fail-");
    ASSERT_FALSE(AboveLowWaterMark(manager, needSize));
    DS_ASSERT_OK(inject::Set("evictAction.setSpill", "call()"));
    DS_ASSERT_OK(inject::Set("worker.SubmitSpillTask", "return(K_NO_SPACE)"));
    Raii clearInjection([]() {
        (void)inject::Clear("evictAction.setSpill");
        (void)inject::Clear("worker.SubmitSpillTask");
    });
    const auto usedBefore = GetAllocatedSize();

    manager->EvictionTaskForTest(needSize, CacheType::MEMORY, true);
    // Every spill failed: no byte was reclaimed, so nothing may be counted as freed and all
    // candidates must be back in the eviction list for the next retry.
    EXPECT_EQ(GetAllocatedSize(), usedBefore);
    EXPECT_EQ(GetEvictionListCount(manager), objectCount);
}

TEST_F(EvictionManagerTest, ForceEvictionCountsSpillOnlyOnFutureSuccess)
{
    std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
    InitEvictionManager(uniqueManager, globalRefs);
    std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
    constexpr uint64_t objectSize = 16 * 1024 * 1024;
    constexpr uint64_t needSize = 1024 * 1024;
    constexpr size_t objectCount = 4;
    CreateObjectsBelowLowWaterMark(manager, objectCount, objectSize, "force-spill-ok-");
    ASSERT_FALSE(AboveLowWaterMark(manager, needSize));
    DS_ASSERT_OK(inject::Set("evictAction.setSpill", "call()"));
    DS_ASSERT_OK(inject::Set("worker.SubmitSpillTask", "return()"));
    Raii clearInjection([]() {
        (void)inject::Clear("evictAction.setSpill");
        (void)inject::Clear("worker.SubmitSpillTask");
    });
    const auto usedBefore = GetAllocatedSize();

    manager->EvictionTaskForTest(needSize, CacheType::MEMORY, true);
    // Successful futures resolve without re-adding candidates (mocked spill frees nothing, so the
    // accounting is future-based); the loop stops without over-draining the list.
    EXPECT_EQ(GetAllocatedSize(), usedBefore);
    EXPECT_LT(GetEvictionListCount(manager), objectCount);
}

TEST_F(EvictionManagerTest, EvictMergesPendingForceNeedIntoRunningTask)
{
    TestEvictMergesPendingForceNeedIntoRunningTask();
}

TEST_F(EvictionManagerTest, PendingForceNeedsAreHeldPerCacheType)
{
    TestPendingForceNeedsAreHeldPerCacheType();
}

TEST_F(EvictionManagerTest, ExpiredPendingForceNeedIsDropped)
{
    TestExpiredPendingForceNeedIsDropped();
}

TEST_F(EvictionManagerTest, MergePendingForceIntoTaskOnlyConsumesOwnCacheType)
{
    TestMergePendingForceIntoTaskOnlyConsumesOwnCacheType();
}

TEST_F(EvictionManagerTest, ReleaseSpillFuturesCountsOnlyReleasedFutures)
{
    TestReleaseSpillFuturesCountsOnlyReleasedFutures();
}

TEST_F(EvictionManagerTest, ResubmitPendingForceEvictionRunsTheTask)
{
    TestResubmitPendingForceEvictionRunsTheTask();
}

TEST_F(EvictionManagerTest, ResubmitPendingForceEvictionSurvivesPoolShutdown)
{
    TestResubmitPendingForceEvictionSurvivesPoolShutdown();
}

TEST_F(EvictionManagerTest, IsExtentUnavailableOomMatchesOnlyExtentReasons)
{
    const std::string arenaOom = "MEMORY no space in arena: 3, reason=";
    EXPECT_TRUE(
        memory::IsExtentUnavailableOom(Status(K_OUT_OF_MEMORY, arenaOom + memory::FRESH_EXTENT_UNAVAILABLE_REASON)));
    EXPECT_TRUE(memory::IsExtentUnavailableOom(
        Status(K_OUT_OF_MEMORY, arenaOom + memory::REUSABLE_EXTENT_UNAVAILABLE_REASON)));
    // The reason token must match exactly; a trailing suffix keeps the token intact.
    EXPECT_TRUE(memory::IsExtentUnavailableOom(
        Status(K_OUT_OF_MEMORY, arenaOom + memory::FRESH_EXTENT_UNAVAILABLE_REASON + ", arena stats")));
    // Status::AppendMsg joins with ". ", so a diagnostic appended downstream must not turn the token
    // into a near miss and silently disable the force path.
    EXPECT_TRUE(memory::IsExtentUnavailableOom(
        Status(K_OUT_OF_MEMORY, arenaOom + memory::FRESH_EXTENT_UNAVAILABLE_REASON + ". arena stats")));
    EXPECT_FALSE(memory::IsExtentUnavailableOom(
        Status(K_OUT_OF_MEMORY, "Upper to the size limit, reason=logical_size_limit_reached")));
    EXPECT_FALSE(
        memory::IsExtentUnavailableOom(Status(K_OUT_OF_MEMORY, "Status inject by worker.Allocator.AllocateMemory")));
    EXPECT_FALSE(memory::IsExtentUnavailableOom(Status::OK()));
    // The code check is the first line of defence: the same text under another code must not qualify.
    EXPECT_FALSE(
        memory::IsExtentUnavailableOom(Status(K_RUNTIME_ERROR, arenaOom + memory::FRESH_EXTENT_UNAVAILABLE_REASON)));
    // Near-miss or missing reason tokens must not act as a control signal.
    EXPECT_FALSE(memory::IsExtentUnavailableOom(
        Status(K_OUT_OF_MEMORY, "MEMORY no space in arena: 3, reason=fresh_extent_unavailabl")));
    EXPECT_FALSE(memory::IsExtentUnavailableOom(
        Status(K_OUT_OF_MEMORY, "MEMORY no space in arena: 3, reason=extent_unavailable")));
    EXPECT_FALSE(memory::IsExtentUnavailableOom(
        Status(K_OUT_OF_MEMORY, "MEMORY no space in arena: 3, reason=FRESH_EXTENT_UNAVAILABLE")));
    EXPECT_FALSE(memory::IsExtentUnavailableOom(
        Status(K_OUT_OF_MEMORY, "MEMORY no space in arena: 3 extent_unavailable diagnostics")));
}

TEST_F(EvictionManagerTest, ExtentOomStatusFromProducerDrivesTheParser)
{
    // Drive the parser with the producer's own output instead of a hand-written string: changing the
    // extent-OOM format in FormatExtentOomMessage now breaks this test rather than silently darkening
    // the force path in production.
    EXPECT_TRUE(memory::IsExtentUnavailableOom(Status(
        K_OUT_OF_MEMORY, memory::FormatExtentOomMessage("Shared memory", 3, /*freshExtentUnavailable=*/true))));
    EXPECT_TRUE(memory::IsExtentUnavailableOom(Status(
        K_OUT_OF_MEMORY, memory::FormatExtentOomMessage("Shared memory", 3, /*freshExtentUnavailable=*/false))));
    auto appended = Status(K_OUT_OF_MEMORY, memory::FormatExtentOomMessage("Shared memory", 3, true));
    appended.AppendMsg("arena stats");
    EXPECT_TRUE(memory::IsExtentUnavailableOom(appended));
}

TEST_F(EvictionManagerTest, AllocateMemoryWiringForcesEvictionBelowLowWaterMark)
{
    std::unique_ptr<WorkerOcEvictionManager> uniqueManager;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
    InitEvictionManager(uniqueManager, globalRefs);
    std::shared_ptr<WorkerOcEvictionManager> manager(std::move(uniqueManager));
    constexpr uint64_t objectSize = 16 * 1024 * 1024;
    constexpr uint64_t requestSize = 1024 * 1024;
    CreateObjectsBelowLowWaterMark(manager, 4, objectSize, "wiring-force-");
    ASSERT_FALSE(AboveLowWaterMark(manager, requestSize));
    const auto usedBefore = GetAllocatedSize();

    auto &timeout = GetRequestContext()->reqTimeoutDuration;
    const auto savedTimeout = timeout;
    Raii clearInjection([&timeout, savedTimeout] {
        (void)inject::Clear("worker.Allocator.AllocateMemory");
        (void)inject::Clear("worker.AllocateMemory.sleepTime");
        (void)inject::Clear("worker.AllocateMemory.forceEvict");
        timeout = savedTimeout;
    });
    DS_ASSERT_OK(inject::Set("worker.Allocator.AllocateMemory", "return(K_OUT_OF_MEMORY)"));
    DS_ASSERT_OK(inject::Set("worker.AllocateMemory.sleepTime", "call(0)"));
    DS_ASSERT_OK(inject::Set("worker.AllocateMemory.forceEvict", "call()"));
    timeout = TimeoutDuration(2000);

    ShmUnit unit;
    auto rc = AllocateMemoryForObject("wiring-force", requestSize, 0, false, manager, unit, CacheType::MEMORY, true);
    EXPECT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);
    // A barrier on the single eviction executor observes completion without racing its asynchronous release.
    DrainEvictionPool(manager);
    // The injected OOM carries no reason token, so only the production call site can have forced this
    // round: without it the low-water gate would have made Evict a no-op.
    EXPECT_LT(GetAllocatedSize(), usedBefore);
    EXPECT_LT(GetEvictionListCount(manager), 4U);
}


TEST_F(EvictionManagerTest, PretriggerMarginAutoReservesAverageObjectSize)
{
    const auto savedHighWatermark = FLAGS_eviction_high_watermark_ratio;
    const auto savedLowWatermark = FLAGS_eviction_low_watermark_ratio;
    const auto savedReserveThreshold = FLAGS_eviction_reserve_mem_threshold_mb;
    allocator->ResetForTest();
    DS_ASSERT_OK(allocator->Init(64 * MB_TO_BYTES));
    FLAGS_eviction_high_watermark_ratio = 0.8;
    FLAGS_eviction_low_watermark_ratio = 0.5;
    FLAGS_eviction_reserve_mem_threshold_mb = 16;
    RefreshWatermarkFactors();

    auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
        objectTable_, HostPort("127.0.0.1", 31501), HostPort("127.0.0.1", 31500), GetTestMetadataRoute());
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_ASSERT_OK(evictionManager->Init(globalRefTable, akSkManager_));
    // Empty cache: the estimate falls back to the 1 MiB floor instead of dividing by zero.
    EXPECT_EQ(1 * MB_TO_BYTES, evictionManager->EstimatePretriggerMarginBytes());

    // 6 x 8 MiB objects: hard watermark is max(0.8 * 64, 64 - 16) = 51.2 MiB, the auto margin is the
    // 8 MiB average, so the patrol fires anywhere in [43.2, 51.2) while the foreground hard-line
    // check stays untouched at 48 MiB usage.
    constexpr uint64_t objectSize = 8 * MB_TO_BYTES;
    for (size_t i = 0; i < 6; ++i) {
        auto key = "auto-pretrigger-" + std::to_string(i);
        DS_ASSERT_OK(CreateObject(key, objectSize - GetMetaSize(objectSize), WriteMode::NONE_L2_CACHE, true, true));
        evictionManager->Add(key);
    }
    EXPECT_GE(evictionManager->EstimatePretriggerMarginBytes(), objectSize);

    EXPECT_FALSE(EvictWhenMemoryExceedThrehold("", 0, evictionManager));
    EXPECT_TRUE(EvictWhenMemoryExceedPretriggerWatermark(evictionManager));

    evictionManager.reset();
    FLAGS_eviction_high_watermark_ratio = savedHighWatermark;
    FLAGS_eviction_low_watermark_ratio = savedLowWatermark;
    FLAGS_eviction_reserve_mem_threshold_mb = savedReserveThreshold;
    RefreshWatermarkFactors();
}

TEST_F(EvictionManagerTest, PretriggerAutoMarginFallsBackToHardWatermark)
{
    const auto savedHighWatermark = FLAGS_eviction_high_watermark_ratio;
    const auto savedLowWatermark = FLAGS_eviction_low_watermark_ratio;
    const auto savedReserveThreshold = FLAGS_eviction_reserve_mem_threshold_mb;
    allocator->ResetForTest();
    DS_ASSERT_OK(allocator->Init(64 * MB_TO_BYTES));
    FLAGS_eviction_high_watermark_ratio = 0.8;
    FLAGS_eviction_low_watermark_ratio = 0.5;
    FLAGS_eviction_reserve_mem_threshold_mb = 16;
    RefreshWatermarkFactors();

    auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
        objectTable_, HostPort("127.0.0.1", 31501), HostPort("127.0.0.1", 31500), GetTestMetadataRoute());
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_ASSERT_OK(evictionManager->Init(globalRefTable, akSkManager_));
    // A single 47 MiB object: the auto margin would push the soft line (4.2 MiB) below the low
    // watermark (32 MiB), so the patrol keeps the hard watermark and stays silent below it.
    DS_ASSERT_OK(CreateObject("single-large-object", 47 * MB_TO_BYTES));
    evictionManager->Add("single-large-object");
    EXPECT_GE(evictionManager->EstimatePretriggerMarginBytes(), 47 * MB_TO_BYTES);

    EXPECT_FALSE(EvictWhenMemoryExceedPretriggerWatermark(evictionManager));

    DS_ASSERT_OK(DeleteObject("single-large-object"));
    evictionManager.reset();
    FLAGS_eviction_high_watermark_ratio = savedHighWatermark;
    FLAGS_eviction_low_watermark_ratio = savedLowWatermark;
    FLAGS_eviction_reserve_mem_threshold_mb = savedReserveThreshold;
    RefreshWatermarkFactors();
}

TEST_F(EvictionManagerTest, PretriggerMarginRoundsUpToCoverAverageObject)
{
    const auto savedHighWatermark = FLAGS_eviction_high_watermark_ratio;
    const auto savedLowWatermark = FLAGS_eviction_low_watermark_ratio;
    const auto savedReserveThreshold = FLAGS_eviction_reserve_mem_threshold_mb;
    allocator->ResetForTest();
    DS_ASSERT_OK(allocator->Init(64 * MB_TO_BYTES));
    FLAGS_eviction_high_watermark_ratio = 0.8;
    FLAGS_eviction_low_watermark_ratio = 0.5;
    FLAGS_eviction_reserve_mem_threshold_mb = 16;
    RefreshWatermarkFactors();

    auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
        objectTable_, HostPort("127.0.0.1", 31501), HostPort("127.0.0.1", 31500), GetTestMetadataRoute());
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_ASSERT_OK(evictionManager->Init(globalRefTable, akSkManager_));
    // 33 x 1.5 MiB objects: the 1.5 MiB average must round up to a 2 MiB margin, putting the soft
    // watermark at 49.2 MiB. A floored 1 MiB margin (soft 50.2 MiB) would leave the 49.5 MiB usage
    // in a residual dead band where neither the patrol nor rebalance can proceed.
    constexpr uint64_t objectSize = 3 * MB_TO_BYTES / 2;
    for (size_t i = 0; i < 33; ++i) {
        auto key = "round-up-pretrigger-" + std::to_string(i);
        DS_ASSERT_OK(CreateObject(key, objectSize - GetMetaSize(objectSize), WriteMode::NONE_L2_CACHE, true, true));
        evictionManager->Add(key);
    }

    EXPECT_FALSE(EvictWhenMemoryExceedThrehold("", 0, evictionManager));
    EXPECT_TRUE(EvictWhenMemoryExceedPretriggerWatermark(evictionManager));

    evictionManager.reset();
    FLAGS_eviction_high_watermark_ratio = savedHighWatermark;
    FLAGS_eviction_low_watermark_ratio = savedLowWatermark;
    FLAGS_eviction_reserve_mem_threshold_mb = savedReserveThreshold;
    RefreshWatermarkFactors();
}

TEST_F(EvictionManagerTest, EvictionRemoveMetaRequestRespectsRedirectPolicy)
{
    TestEvictionRemoveMetaRequestRespectsRedirectPolicy();
}

TEST_F(EvictionManagerTest, EvictionRedirectStopsAtForwardedTarget)
{
    TestEvictionRedirectStopsAtForwardedTarget();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeRedirectForwardsOnceAndPreservesRequest)
{
    TestPrimaryEndLifeRedirectForwardsOnceAndPreservesRequest();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeSecondRedirectStopsAtTarget)
{
    TestPrimaryEndLifeSecondRedirectStopsAtTarget();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeMixedRedirectResultOnlyReaddsFailedKeys)
{
    TestPrimaryEndLifeMixedRedirectResultOnlyReaddsFailedKeys();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeRedirectTargetTimeoutDoesNotForceDelete)
{
    TestPrimaryEndLifeRedirectTargetTimeoutDoesNotForceDelete();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeMalformedRedirectFailsClosed)
{
    TestPrimaryEndLifeMalformedRedirectFailsClosed();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeSourceTimeoutUsesOneRpcPerCall)
{
    TestPrimaryEndLifeSourceTimeoutUsesOneRpcPerCall();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeRedirectResetsSourceRetryState)
{
    VerifyPrimaryEndLifeRedirectResetsSourceRetryState();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeRouteFailureReleasesPendingSlot)
{
    VerifyPrimaryEndLifeRouteFailureReleasesPendingSlot();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeOwnerReleaseIsNoThrow)
{
    VerifyPrimaryEndLifeOwnerReleaseIsNoThrow();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeAcquireRollbackBeforeCommit)
{
    VerifyPrimaryEndLifeAcquireRollbackBeforeCommit();
}

TEST_F(EvictionManagerTest, PrimaryEndLifeSameOwnerSingleFlightDoesNotBlockHealthyOwner)
{
    const HostPort ownerA("127.0.0.1", 31502);
    const HostPort ownerB("127.0.0.1", 31503);
    std::atomic<size_t> ownerACalls{ 0 };
    std::atomic<size_t> ownerBCalls{ 0 };
    std::promise<void> ownerAStarted;
    auto ownerAStartedFuture = ownerAStarted.get_future();
    std::promise<void> releaseOwnerA;
    auto releaseOwnerAFuture = releaseOwnerA.get_future().share();
    std::atomic<bool> ownerAStartSignaled{ false };
    std::atomic<bool> ownerAReleased{ false };
    std::promise<void> ownerBCompleted;
    auto ownerBCompletedFuture = ownerBCompleted.get_future();
    std::atomic<bool> ownerBCompletionSignaled{ false };
    auto ownerAApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
        ownerA, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &) {
            ++ownerACalls;
            if (!ownerAStartSignaled.exchange(true)) {
                ownerAStarted.set_value();
                releaseOwnerAFuture.wait();
            }
            return Status::OK();
        });
    auto ownerBApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
        ownerB, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &) {
            ++ownerBCalls;
            if (!ownerBCompletionSignaled.exchange(true)) {
                ownerBCompleted.set_value();
            }
            return Status::OK();
        });
    BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                   (testing::_, testing::_, testing::_, testing::_))
        .WillRepeatedly(testing::Invoke(
            [&](const HostPort &masterAddress, const HostPort &, std::shared_ptr<AkSkManager>,
                master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> {
                return masterAddress == ownerB ? ownerBApi : ownerAApi;
            }));

    cluster::TopologySnapshotState snapshots;
    DS_ASSERT_OK(PublishPrimaryEndLifeTopology(snapshots, 1, cluster::MemberState::ACTIVE));
    PrimaryEndLifeRoutingAlgorithm algorithm;
    cluster::PlacementFacade placement(snapshots, algorithm, "127.0.0.1:31501");
    worker::MetadataRouteResolver route(&placement, worker::MetadataRouteOptions{});
    {
        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501), ownerA, route);
        Raii releaseBlockedOwner([&] {
            if (!ownerAReleased.exchange(true)) {
                releaseOwnerA.set_value();
            }
        });
        StartPrimaryEndLifeWorkersForTest(manager);
        DS_ASSERT_OK(CreateObject("owner-a-first", TEST_DATA_SIZE, WriteMode::NONE_L2_CACHE_EVICT));
        DS_ASSERT_OK(CreateObject("owner-a-second", TEST_DATA_SIZE, WriteMode::NONE_L2_CACHE_EVICT));
        DS_ASSERT_OK(CreateObject("owner-b-healthy", TEST_DATA_SIZE, WriteMode::NONE_L2_CACHE_EVICT));
        DS_ASSERT_OK(SubmitPrimaryEndLifeTaskForTest(manager, "owner-a-first"));
        ASSERT_EQ(ownerAStartedFuture.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        DS_ASSERT_OK(SubmitPrimaryEndLifeTaskForTest(manager, "owner-a-second"));
        DS_ASSERT_OK(SubmitPrimaryEndLifeTaskForTest(manager, "owner-b-healthy"));

        EXPECT_EQ(ownerBCompletedFuture.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        EXPECT_EQ(ownerACalls.load(), 1U);
        if (!ownerAReleased.exchange(true)) {
            releaseOwnerA.set_value();
        }
        EXPECT_TRUE(WaitUntil(
            [&] {
                return ownerACalls.load() == 2 && ownerBCalls.load() == 1
                       && !objectTable_->Contains("owner-a-first") && !objectTable_->Contains("owner-a-second")
                       && !objectTable_->Contains("owner-b-healthy");
            },
            std::chrono::seconds(2)));
    }
    RELEASE_STUBS
}

TEST_F(EvictionManagerTest, PrimaryEndLifeRetryIsDeferredAcrossDrainRounds)
{
    const HostPort owner("127.0.0.1", 31500);
    std::atomic<size_t> rpcCalls{ 0 };
    std::mutex attemptMutex;
    std::vector<std::chrono::steady_clock::time_point> attemptTimes;
    auto ownerApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
        owner, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &) {
            ++rpcCalls;
            {
                std::lock_guard<std::mutex> lock(attemptMutex);
                attemptTimes.emplace_back(std::chrono::steady_clock::now());
            }
            return Status(K_RPC_DEADLINE_EXCEEDED, "source timeout");
        });
    BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                   (testing::_, testing::_, testing::_, testing::_))
        .WillRepeatedly(testing::Return(ownerApi));

    {
        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501), owner,
                                        GetTestMetadataRoute());
        StartPrimaryEndLifeWorkersForTest(manager);
        DS_ASSERT_OK(CreateObject("deferred-source-retry", TEST_DATA_SIZE, WriteMode::NONE_L2_CACHE_EVICT));
        DS_ASSERT_OK(CreateObject("metadata-already-deleted", TEST_DATA_SIZE, WriteMode::NONE_L2_CACHE_EVICT));
        MarkPrimaryEndLifeMetadataDeletedForTest(manager, "metadata-already-deleted");
        DS_ASSERT_OK(SubmitPrimaryEndLifeTaskForTest(manager, "deferred-source-retry"));
        DS_ASSERT_OK(SubmitPrimaryEndLifeTaskForTest(manager, "metadata-already-deleted"));
        ASSERT_TRUE(WaitUntil([&] { return rpcCalls.load() >= 1; }, std::chrono::seconds(2)));
        EXPECT_TRUE(WaitUntil([&] { return !objectTable_->Contains("metadata-already-deleted"); },
                              std::chrono::seconds(2)));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        EXPECT_EQ(rpcCalls.load(), 1U);
        EXPECT_TRUE(WaitUntil(
            [&] { return rpcCalls.load() == 3 && !objectTable_->Contains("deferred-source-retry"); },
            std::chrono::seconds(2)));
        EXPECT_EQ(rpcCalls.load(), 3U);
    }
    RELEASE_STUBS

    std::lock_guard<std::mutex> lock(attemptMutex);
    ASSERT_EQ(attemptTimes.size(), 3U);
    EXPECT_GE(attemptTimes[1] - attemptTimes[0], std::chrono::milliseconds(80));
    EXPECT_GE(attemptTimes[2] - attemptTimes[1], std::chrono::milliseconds(80));
}

TEST_F(EvictionManagerTest, PrimaryEndLifeReroutesAfterFailedOwnerIsIsolated)
{
    const HostPort failedOwner("127.0.0.1", 31502);
    const HostPort recoveryOwner("127.0.0.1", 31503);
    std::atomic<size_t> failedOwnerCalls{ 0 };
    std::atomic<size_t> recoveryOwnerCalls{ 0 };
    std::promise<void> failedOwnerStarted;
    auto failedOwnerStartedFuture = failedOwnerStarted.get_future();
    std::promise<void> releaseFailedOwner;
    auto releaseFailedOwnerFuture = releaseFailedOwner.get_future().share();
    std::atomic<bool> failedOwnerStartSignaled{ false };
    std::atomic<bool> failedOwnerReleased{ false };
    auto failedOwnerApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
        failedOwner, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &) {
            ++failedOwnerCalls;
            if (!failedOwnerStartSignaled.exchange(true)) {
                failedOwnerStarted.set_value();
            }
            releaseFailedOwnerFuture.wait();
            return Status(K_RPC_DEADLINE_EXCEEDED, "old owner timeout");
        });
    auto recoveryOwnerApi = std::make_shared<FakeDeleteAllCopyMetaMasterApi>(
        recoveryOwner, [&](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &) {
            ++recoveryOwnerCalls;
            return Status::OK();
        });
    BINEXPECT_CALL(&worker::WorkerMasterOCApi::CreateWorkerMasterOCApi,
                   (testing::_, testing::_, testing::_, testing::_))
        .WillRepeatedly(testing::Invoke(
            [&](const HostPort &masterAddress, const HostPort &, std::shared_ptr<AkSkManager>,
                master::MasterOCServiceImpl *) -> std::shared_ptr<worker::WorkerMasterOCApi> {
                return masterAddress == recoveryOwner ? recoveryOwnerApi : failedOwnerApi;
            }));

    cluster::TopologySnapshotState snapshots;
    DS_ASSERT_OK(PublishPrimaryEndLifeTopology(snapshots, 1, cluster::MemberState::ACTIVE));
    PrimaryEndLifeRoutingAlgorithm algorithm;
    cluster::PlacementFacade placement(snapshots, algorithm, "127.0.0.1:31501");
    worker::MetadataRouteResolver route(&placement, worker::MetadataRouteOptions{});
    {
        WorkerOcEvictionManager manager(objectTable_, HostPort("127.0.0.1", 31501), failedOwner, route);
        Raii releaseBlockedOwner([&] {
            if (!failedOwnerReleased.exchange(true)) {
                releaseFailedOwner.set_value();
            }
        });
        StartPrimaryEndLifeWorkersForTest(manager);
        DS_ASSERT_OK(CreateObject("owner-a-reroute", TEST_DATA_SIZE, WriteMode::NONE_L2_CACHE_EVICT));
        DS_ASSERT_OK(SubmitPrimaryEndLifeTaskForTest(manager, "owner-a-reroute"));
        ASSERT_EQ(failedOwnerStartedFuture.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        DS_ASSERT_OK(PublishPrimaryEndLifeTopology(snapshots, 2, cluster::MemberState::FAILED));
        if (!failedOwnerReleased.exchange(true)) {
            releaseFailedOwner.set_value();
        }

        EXPECT_TRUE(WaitUntil(
            [&] { return recoveryOwnerCalls.load() == 1 && !objectTable_->Contains("owner-a-reroute"); },
            std::chrono::seconds(2)));
        EXPECT_EQ(failedOwnerCalls.load(), 1U);
        EXPECT_EQ(recoveryOwnerCalls.load(), 1U);
    }
    RELEASE_STUBS
}

TEST_F(EvictionManagerTest, PrimaryEndLifeReacquireRejectsObjectClaimedByRebalance)
{
    std::unique_ptr<WorkerOcEvictionManager> manager;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
    InitEvictionManager(manager, globalRefs);
    const std::string objectKey = "primary-end-life-rebalance-window";
    auto object = std::make_unique<object_cache::ObjCacheShmUnit>();
    object->SetCreateTime(1);
    object->SetLifeState(ObjectLifeState::OBJECT_SEALED);
    object->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE_EVICT);
    object->modeInfo.SetCacheType(CacheType::MEMORY);
    DS_ASSERT_OK(objectTable_->Insert(objectKey, std::move(object)));
    manager->Add(objectKey);
    ASSERT_TRUE(manager->TryMarkRebalancingObject(objectKey));

    std::shared_ptr<SafeObjType> lockedEntry;
    auto rc = manager->ReacquirePrimaryEndLifeForTest(objectKey, 1, lockedEntry);
    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);
    EXPECT_EQ(lockedEntry, nullptr);
    EXPECT_TRUE(objectTable_->Contains(objectKey).IsOk());
    manager->UnmarkRebalancingObject(objectKey);
}

TEST_F(EvictionManagerTest, NoneL2FallbackRedirectForwardsOnce)
{
    TestNoneL2FallbackRedirectForwardsOnce();
}

TEST_F(EvictionManagerTest, NoneL2FallbackSecondRedirectKeepsLocalObject)
{
    TestNoneL2FallbackSecondRedirectKeepsLocalObject();
}

TEST_F(EvictionManagerTest, AddTracksObjectTableAndEvictionCounters)
{
    std::unique_ptr<WorkerOcEvictionManager> manager;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
    InitEvictionManager(manager, globalRefs);
    AddThreeTrackedObjects(*manager, globalRefs);

    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> tableObjects;
    GetAllObjsFromObjectTable(tableObjects);
    ASSERT_EQ(tableObjects.size(), 3U);
    EXPECT_EQ((*tableObjects["id1"])->GetDataSize(), TEST_DATA_SIZE);
    EXPECT_EQ((*tableObjects["id2"])->GetDataSize(), TEST_DATA_SIZE);
    EXPECT_EQ((*tableObjects["id3"])->GetDataSize(), TEST_DATA_SIZE);
    std::vector<EvictionList::Node> listObjects;
    EvictionList::Node oldest;
    DS_ASSERT_OK(manager->GetAllObjectsInfo(listObjects, oldest));
    ASSERT_EQ(listObjects.size(), 3U);
    EXPECT_EQ(listObjects[0].curCounter, 1U);
    EXPECT_EQ(listObjects[1].curCounter, 1U);
    EXPECT_EQ(listObjects[2].curCounter, 2U);
    EXPECT_EQ(oldest.objectKey, "id1");
    DeleteThreeObjects();
}

TEST_F(EvictionManagerTest, EraseUpdatesEvictionListAndObjectTable)
{
    std::unique_ptr<WorkerOcEvictionManager> manager;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefs;
    InitEvictionManager(manager, globalRefs);
    AddThreeTrackedObjects(*manager, globalRefs);
    std::vector<EvictionList::Node> listObjects;
    EvictionList::Node oldest;

    manager->Erase("id1");
    DS_ASSERT_OK(manager->GetAllObjectsInfo(listObjects, oldest));
    ASSERT_EQ(listObjects.size(), 2U);
    EXPECT_EQ(listObjects[0].objectKey, "id2");
    EXPECT_EQ(listObjects[1].objectKey, "id3");
    listObjects.clear();
    manager->Erase("id2");
    DS_ASSERT_OK(manager->GetAllObjectsInfo(listObjects, oldest));
    ASSERT_EQ(listObjects.size(), 1U);
    EXPECT_EQ(listObjects[0].objectKey, "id3");
    listObjects.clear();
    manager->Erase("id3");
    DS_ASSERT_OK(manager->GetAllObjectsInfo(listObjects, oldest));
    EXPECT_TRUE(listObjects.empty());
    DeleteThreeObjects();
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> tableObjects;
    GetAllObjsFromObjectTable(tableObjects);
    EXPECT_TRUE(tableObjects.empty());
}

class ScEvictionObjectTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        LOG(INFO) << "Init ScEvictionObjectTest";
    }

    void TearDown() override
    {
        scAllocateManager_.reset();
        evictionManager_.reset();
        objectTable_.reset();
        WorkerOcSpill::Instance()->ResetForTest();
        if (allocator != nullptr) {
            allocator->ResetForTest();
            allocator = nullptr;
        }
        CommonTest::TearDown();
    }

    void InitTest()
    {
        scAllocateManager_.reset();
        evictionManager_.reset();
        objectTable_.reset();
        WorkerOcSpill::Instance()->ResetForTest();
        if (allocator != nullptr) {
            allocator->ResetForTest();
        }
        objectTable_ = std::make_shared<ObjectTable>();
        allocator = datasystem::memory::Allocator::Instance();
        akSkManager_ = std::make_shared<AkSkManager>(0);

        allocator->Init(maxSize_, 0, false, true, 5000, ocPercent_, scPercent_);  // decay is 5000 ms.
        std::shared_ptr<ObjectTable> &objectTable = GetObjectTable();
        evictionManager_ = std::make_shared<object_cache::WorkerOcEvictionManager>(
            objectTable, HostPort("127.0.0.1", 32131),  // worker port is 32131,
            HostPort("127.0.0.1", 52319), GetTestMetadataRoute());  // master port is 52319;
        auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        DS_ASSERT_OK(evictionManager_->Init(globalRefTable, akSkManager_));
        scAllocateManager_ = std::make_shared<worker::stream_cache::WorkerSCAllocateMemory>(evictionManager_);
    }

    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<object_cache::WorkerOcEvictionManager> evictionManager_;
    std::shared_ptr<worker::stream_cache::WorkerSCAllocateMemory> scAllocateManager_;
    uint64_t maxSize_ = 0;
    int scPercent_ = 0;
    int ocPercent_ = 0;
    int limit = 100 * 1024 * 1024;  // spill limit size is 100 * 1024 * 1024;
};

TEST_F(ScEvictionObjectTest, DISABLED_TestEvictSc50Oc50)
{
    maxSize_ = 50 * 1024 * 1024;                 // shared memory size 50 * 1024 * 1024
    scPercent_ = 70;                             // sc shared memory max size is 70 / 100 * maxSize_
    ocPercent_ = 100;                            // oc shared memory max size is 50 / 100 * maxSize_
    constexpr size_t limit = 100 * 1024 * 1024;  //
    FLAGS_spill_size_limit = limit;
    FLAGS_spill_directory = "./spill_TestEvictSc50Oc50";
    InitTest();
    auto streamSize = 1 * 1024 * 1024;  // stream page size is 1 * 1024 * 1024;
    for (int i = 0; i < 30; i++) {      // object num is 30
        auto prefix = "test_for_evict_";
        auto objectSize = 1 * 1024 * 1024;
        DS_ASSERT_OK(CreateObject(prefix + std::to_string(i), objectSize));
        evictionManager_->Add(prefix + std::to_string(i));
    }
    auto unit = std::make_shared<ShmUnit>();
    for (int i = 0; i < 30; i++) {  // stream num is 30
        DS_ASSERT_OK(scAllocateManager_->AllocateMemoryForStream(DEFAULT_TENANT_ID, "qwer" + std::to_string(i),
                                                                 streamSize, true, *unit, true));
    }
}

TEST_F(ScEvictionObjectTest, TestEvictScSizeMax)
{
    maxSize_ = 50 * 1024 * 1024;  // shared memory size 50 * 1024 * 1024
    scPercent_ = 50;              // sc shared memory max size is 50% * maxSize_
    ocPercent_ = 100;             // oc shared memory max size is 100% * maxSize_
    FLAGS_spill_size_limit = limit;
    FLAGS_spill_directory = "./spill_TestEvictScSizeMax";
    InitTest();
    auto size = 27 * 1024 * 1024;  // stream page size is 27 * 1024 * 1024;
    auto unit = std::make_shared<ShmUnit>();
    auto status = unit->AllocateMemory("", size, true, ServiceType::STREAM);
    ASSERT_EQ(status.GetCode(), StatusCode::K_OUT_OF_MEMORY) << status.GetMsg();
    status = scAllocateManager_->AllocateMemoryForStream(DEFAULT_TENANT_ID, "qwer", size, true, *unit, true);
    ASSERT_TRUE(status.GetMsg().find("Stream cache memory size overflow, maxStreamSize") != std::string::npos);
}

TEST_F(ScEvictionObjectTest, TestScNotEvictObject)
{
    maxSize_ = 50 * 1024 * 1024;  // shared memory size 50 * 1024 * 1024
    scPercent_ = 100;             // sc shared memory max size is 100% * maxSize_
    ocPercent_ = 100;             // oc shared memory max size is 100% * maxSize_
    FLAGS_spill_size_limit = limit;
    FLAGS_spill_directory = "./spill_TestScNotEvictObject";
    auto streamSize = 2 * 1024 * 1024;  // stream page size is 2 * 1024 * 1024;
    InitTest();
    auto unit = std::make_shared<ShmUnit>();
    for (int i = 0; i < 9; i++) {  // stream page num is 9
        DS_ASSERT_OK(scAllocateManager_->AllocateMemoryForStream(DEFAULT_TENANT_ID, "qwer" + std::to_string(i),
                                                                 streamSize, true, *unit, true));
    }
}

TEST_F(ScEvictionObjectTest, TestEvictObject)
{
    LOG_IF_ERROR(inject::Set("worker.Spill.Sync", "return()"), "set inject point failed");
    maxSize_ = 10 * 1024 * 1024;                 // shared memory size 10 * 1024 * 1024
    scPercent_ = 100;                            // sc shared memory max size is 100% * maxSize_
    ocPercent_ = 50;                             // oc shared memory max size is 50% * maxSize_
    constexpr size_t limit = 100 * 1024 * 1024;  // spill limit size is 100 * 1024 * 1024;
    FLAGS_spill_size_limit = limit;
    FLAGS_spill_directory = "./spill_TestEvictObject";
    auto streamSize = 8 * 1024 * 1024;
    InitTest();
    auto unit = std::make_shared<ShmUnit>();
    DS_ASSERT_OK(scAllocateManager_->AllocateMemoryForStream(DEFAULT_TENANT_ID, "qwer", streamSize, true, *unit, true));
    const int kNumObjectsToCreate = 10;
    for (int i = 0; i < kNumObjectsToCreate; i++) {
        auto prefix = "test_for_evict_";
        auto objectSize = 500 * 1024;
        DS_ASSERT_OK(CreateObject(prefix + std::to_string(i), objectSize, WriteMode::NONE_L2_CACHE, true, false,
                                  DataFormat::BINARY, true, evictionManager_));
        evictionManager_->Add(prefix + std::to_string(i));
    }
}

class EvictionManagerBenchTest : public CommonTest, public BenchHelper {};

TEST_F(EvictionManagerBenchTest, BenchThread1)
{
    const int logLevel = 2;
    FLAGS_minloglevel = logLevel;
    EvictionList list;
    const int threadCnt = 1;
    PerfTwoAction(
        threadCnt, GenUniqueString, [&list](const std::string &key) { list.Add(key, Q1); },
        [&list](const std::string &key) { list.Erase(key); });
}

TEST_F(EvictionManagerBenchTest, BenchThread4)
{
    const int logLevel = 2;
    FLAGS_minloglevel = logLevel;
    EvictionList list;
    const int threadCnt = 4;
    PerfTwoAction(
        threadCnt, GenUniqueString, [&list](const std::string &key) { list.Add(key, Q1); },
        [&list](const std::string &key) { list.Erase(key); });
}

TEST_F(EvictionManagerBenchTest, BenchThread8)
{
    const int logLevel = 2;
    FLAGS_minloglevel = logLevel;
    EvictionList list;
    const int threadCnt = 8;
    PerfTwoAction(
        threadCnt, GenUniqueString, [&list](const std::string &key) { list.Add(key, Q1); },
        [&list](const std::string &key) { list.Erase(key); });
}
}  // namespace ut
}  // namespace datasystem
