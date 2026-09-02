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

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <future>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/object_cache/data_migrator/handler/async_resource_releaser.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"
#include "datasystem/worker/object_cache/rebalance_candidate_provider.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/rebalance_executor.h"
#include "eviction_manager_common.h"
#include "ut/common.h"
#include "test_metadata_route.h"

using namespace datasystem::object_cache;
using namespace datasystem::worker;

namespace datasystem {
namespace ut {
namespace {
constexpr uint64_t MB = 1024ul * 1024ul;
constexpr uint64_t TASK_TIMEOUT_MS = 60'000;
constexpr size_t TOPOLOGY_MEMBER_ID_SIZE = 16;
constexpr size_t TOPOLOGY_DIGEST_SIZE = 64;
constexpr uint32_t TARGET_TOPOLOGY_TOKEN = 2;
const HostPort LOCAL_ADDR("127.0.0.1", 31501);
const HostPort MASTER_ADDR("127.0.0.1", 31500);
const std::string TARGET_ADDR = "127.0.0.1:31502";
const std::string JOINING_ADDR = "127.0.0.1:31503";

master::RebalanceTaskPb MakeTask(const std::string &taskId, uint64_t maxBytes)
{
    master::RebalanceTaskPb task;
    task.set_task_id(taskId);
    task.set_source_worker(LOCAL_ADDR.ToString());
    task.set_target_worker(TARGET_ADDR);
    task.set_max_bytes(maxBytes);
    auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    task.set_create_time_ms(nowMs);
    task.set_timeout_ms(TASK_TIMEOUT_MS);
    task.set_deadline_ms(nowMs + TASK_TIMEOUT_MS);
    task.set_source_eviction_policy(master::EVICTION_POLICY_CLOCK);
    task.set_source_eviction_policy_epoch(0);
    task.set_target_eviction_policy(master::EVICTION_POLICY_CLOCK);
    task.set_target_eviction_policy_epoch(0);
    task.set_has_eviction_policy_fence(true);
    return task;
}

Status SelectCandidates(RebalanceCandidateProvider &provider, uint64_t targetBytes, size_t maxObjectCount,
                        std::unordered_map<std::string, uint64_t> &candidates,
                        RebalanceCandidateProvider::ObjectHeatMap &objectHeats)
{
    RebalanceCandidateSession session;
    return provider.Select(session, targetBytes, maxObjectCount, candidates, objectHeats);
}

Status SelectCandidates(RebalanceCandidateProvider &provider, uint64_t targetBytes, size_t maxObjectCount,
                        std::unordered_map<std::string, uint64_t> &candidates)
{
    RebalanceCandidateProvider::ObjectHeatMap ignoredHeats;
    return SelectCandidates(provider, targetBytes, maxObjectCount, candidates, ignoredHeats);
}
}  // namespace

TEST(WorkerOcSpillLifecycleTest, RepeatedInitIsIdempotent)
{
    Raii reset([]() { WorkerOcSpill::Instance()->ResetForTest(); });
    constexpr size_t threadCount = 16;
    std::atomic<size_t> failures{ 0 };
    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i) {
        threads.emplace_back([&failures]() {
            if (WorkerOcSpill::Instance()->Init().IsError()) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(failures.load(std::memory_order_relaxed), size_t(0));
    DS_ASSERT_OK(WorkerOcSpill::Instance()->Init());
}

TEST(NodeSelectorMaintenanceTest, PreReportHookHonorsMinimumIntervalDuringFastRetries)
{
    auto &selector = NodeSelector::Instance();
    selector.UnregisterPreReportHooks();
    Raii cleanup([&selector]() { selector.UnregisterPreReportHooks(); });
    std::atomic<uint64_t> runs{ 0 };
    selector.RegisterPreReportHook([&runs]() { runs.fetch_add(1, std::memory_order_relaxed); }, 1'000);

    selector.RunPreReportHooksForTest(100);
    selector.RunPreReportHooksForTest(600);
    EXPECT_EQ(runs.load(std::memory_order_relaxed), 1u);
    selector.RunPreReportHooksForTest(1'100);
    EXPECT_EQ(runs.load(std::memory_order_relaxed), 2u);
}

TEST(NodeSelectorMaintenanceTest, ObjectCopyWatermarkFormatsRatiosAndZeroDenominators)
{
    ObjectCopyWatermark empty;
    EXPECT_EQ(empty.ToMetricsString(),
              "0/0/0/0.000000000/0.000000000/0.000000000/0/0/0/0.000000000/0.000000000");

    auto &selector = NodeSelector::Instance();
    selector.SetObjectCopyWatermark(30, 45, 2, 8, 25, 100, 200);
    selector.SetHotPrimaryReport(9, 9, 900, 900, 1'000);
    const auto snapshot = selector.GetObjectCopyWatermark();
    const auto heatReport = selector.GetHotPrimaryReportForTest();
    EXPECT_TRUE(snapshot.valid);
    EXPECT_EQ(snapshot.hotPrimaryCopyCount, 2u);
    EXPECT_EQ(snapshot.totalPrimaryCopyCount, 8u);
    EXPECT_EQ(snapshot.coldPrimaryCopyBytes, 30u);
    EXPECT_EQ(snapshot.warmPrimaryCopyBytes, 45u);
    EXPECT_EQ(snapshot.ToMetricsString(),
              "25/100/200/0.125000000/0.500000000/0.250000000/1/30/45/0.300000000/0.450000000");
    EXPECT_EQ(heatReport.hotPrimaryCopyCount, 9u);
    EXPECT_EQ(heatReport.totalPrimaryCopyCount, 9u);
    EXPECT_EQ(heatReport.hotPrimaryCopyBytes, 900u);
    EXPECT_EQ(heatReport.totalPrimaryCopyBytes, 900u);
    EXPECT_EQ(heatReport.memoryCapacity, 1'000u);
}

TEST(NodeSelectorMaintenanceTest, ControlStateSupportsConcurrentUpdatesAndSnapshots)
{
    auto &selector = NodeSelector::Instance();
    selector.UnregisterPreReportHooks();
    selector.SetObjectCopyWatermark(0, 0, 0, 0, 0, 0, 0);
    selector.SetHotPrimaryReport(0, 0, 0, 0, 0);
    Raii cleanup([&selector]() {
        selector.UnregisterPreReportHooks();
        selector.SetEvictionPolicyReport(master::EVICTION_POLICY_CLOCK, 0, master::EVICTION_POLICY_STABLE);
        selector.SetEvictionPolicyControlReport(master::EVICTION_POLICY_WORKER_NONE, 0, 0, 0);
        selector.SetObjectCopyWatermark(0, 0, 0, 0, 0, 0, 0);
        selector.SetHotPrimaryReport(0, 0, 0, 0, 0);
    });
    std::atomic<bool> start{ false };
    std::atomic<uint64_t> inconsistentSnapshots{ 0 };
    std::atomic<uint64_t> hookRuns{ 0 };

    auto waitForStart = [&start]() {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    };
    std::thread watermarkWriter([&]() {
        waitForStart();
        for (uint64_t value = 1; value <= 2'000; ++value) {
            selector.SetObjectCopyWatermark(value, value, value, value, value, value, value);
            selector.SetHotPrimaryReport(value, value, value, value, value);
        }
    });
    std::thread policyWriter([&]() {
        waitForStart();
        for (uint64_t epoch = 1; epoch <= 2'000; ++epoch) {
            selector.SetEvictionPolicyReport(master::EVICTION_POLICY_HEAT, epoch, master::EVICTION_POLICY_STABLE);
            selector.SetEvictionPolicyControlReport(master::EVICTION_POLICY_WORKER_ACTIVE, epoch, epoch, epoch);
        }
    });
    std::thread hookWriter([&]() {
        waitForStart();
        for (uint64_t i = 0; i < 200; ++i) {
            selector.RegisterPreReportHook(
                [&hookRuns]() { hookRuns.fetch_add(1, std::memory_order_relaxed); });
            selector.RunPreReportHooksForTest(i);
            selector.UnregisterPreReportHooks();
        }
    });
    std::thread reader([&]() {
        waitForStart();
        for (uint64_t i = 0; i < 2'000; ++i) {
            const auto snapshot = selector.GetObjectCopyWatermark();
            if (snapshot.hotPrimaryCopyCount != snapshot.totalPrimaryCopyCount
                || snapshot.hotPrimaryCopyCount != snapshot.hotPrimaryCopyBytes
                || snapshot.hotPrimaryCopyCount != snapshot.totalPrimaryCopyBytes
                || snapshot.hotPrimaryCopyCount != snapshot.memoryCapacity) {
                inconsistentSnapshots.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });
    start.store(true, std::memory_order_release);
    watermarkWriter.join();
    policyWriter.join();
    hookWriter.join();
    reader.join();
    EXPECT_EQ(inconsistentSnapshots.load(std::memory_order_relaxed), 0u);
    EXPECT_GT(hookRuns.load(std::memory_order_relaxed), 0u);
}

class RebalanceCandidateProviderTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        objectTable_ = std::make_shared<ObjectTable>();
        allocator = memory::Allocator::Instance();
        allocator->Init(maxMemorySize);
        akSkManager_ = std::make_shared<AkSkManager>(0);
        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, LOCAL_ADDR, MASTER_ADDR,
                                                                     GetTestMetadataRoute());
        globalRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        DS_ASSERT_OK(evictionManager_->Init(globalRefTable_, akSkManager_));
    }

    void TearDown() override
    {
        evictionManager_.reset();
        globalRefTable_.reset();
        akSkManager_.reset();
        objectTable_.reset();
        allocator->ResetForTest();
        allocator = nullptr;
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

// Verifies that the candidate provider scans from the oldest eviction-list entry and keeps the selected bytes within
// the requested target. This protects the LRU-style rebalance candidate order and the hard batch-size boundary.
TEST_F(RebalanceCandidateProviderTest, SelectCandidatesFromOldestUntilTargetBytes)
{
    CreateAndAdd("oldest", 10 * MB);
    CreateAndAdd("middle", 20 * MB);
    CreateAndAdd("newest", 30 * MB);

    MemoryRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    DS_ASSERT_OK(SelectCandidates(provider, 40 * MB, 10, candidates));

    ASSERT_EQ(candidates.size(), size_t(2));
    // Candidate sizing uses sallocx real size (>= dataSize), so assert >= rather than == dataSize exactly.
    EXPECT_GE(candidates["oldest"], 10 * MB);
    EXPECT_GE(candidates["middle"], 20 * MB);
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

    MemoryRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    DS_ASSERT_OK(SelectCandidates(provider, 40 * MB, 10, candidates));

    ASSERT_EQ(candidates.size(), size_t(1));
    EXPECT_GE(candidates["candidate"], 30 * MB);
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("non_primary"));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("already_rebalancing"));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("candidate"));
}

TEST_F(RebalanceCandidateProviderTest, BusyObjectDoesNotBlockCandidateSelection)
{
    CreateAndAdd("busy", 10 * MB);
    CreateAndAdd("available", 10 * MB);
    std::shared_ptr<SafeObjType> busyEntry;
    DS_ASSERT_OK(objectTable_->Get("busy", busyEntry));
    DS_ASSERT_OK(busyEntry->WLock(true));
    Raii unlock([&busyEntry]() { busyEntry->WUnlock(); });

    MemoryRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    auto selection = std::async(
        std::launch::async, [&provider, &candidates]() { return SelectCandidates(provider, 20 * MB, 10, candidates); });

    EXPECT_EQ(selection.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    DS_ASSERT_OK(selection.get());
    EXPECT_EQ(candidates.count("busy"), size_t(0));
    EXPECT_EQ(candidates.count("available"), size_t(1));
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("busy"));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("available"));
}

TEST_F(RebalanceCandidateProviderTest, ConcurrentMarkHasSingleWinnerPerObject)
{
    constexpr size_t threadCount = 32;
    std::atomic<bool> start{ false };
    std::atomic<size_t> winners{ 0 };
    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i) {
        threads.emplace_back([this, &start, &winners]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            if (evictionManager_->TryMarkRebalancingObject("same_object")) {
                winners.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(winners.load(std::memory_order_relaxed), 1u);
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("same_object"));
    evictionManager_->UnmarkRebalancingObject("same_object");
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("same_object"));
}

TEST_F(RebalanceCandidateProviderTest, ConcurrentDifferentKeyMarkQueryAndUnmark)
{
    constexpr size_t threadCount = 16;
    constexpr size_t keysPerThread = 64;
    std::atomic<bool> start{ false };
    std::atomic<size_t> failures{ 0 };
    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (size_t threadId = 0; threadId < threadCount; ++threadId) {
        threads.emplace_back([this, threadId, &start, &failures]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            std::vector<std::string> keys;
            keys.reserve(keysPerThread);
            for (size_t keyId = 0; keyId < keysPerThread; ++keyId) {
                keys.emplace_back("rebalance_" + std::to_string(threadId) + "_" + std::to_string(keyId));
                if (!evictionManager_->TryMarkRebalancingObject(keys.back())
                    || !evictionManager_->IsObjectBeingRebalanced(keys.back())) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
            for (const auto &key : keys) {
                evictionManager_->UnmarkRebalancingObject(key);
            }
            for (const auto &key : keys) {
                if (evictionManager_->IsObjectBeingRebalanced(key)) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0u);
}

// Verifies an object larger than the remaining byte budget is skipped and unmarked, while a later fitting object can
// still be selected. The provider must never over-reserve the target's capacity/inflight budget.
TEST_F(RebalanceCandidateProviderTest, MemoryProviderSkipsObjectLargerThanRoundBudget)
{
    CreateAndAdd("oversized", 20 * MB);
    CreateAndAdd("fits", 5 * MB);

    MemoryRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    DS_ASSERT_OK(SelectCandidates(provider, 10 * MB, 10, candidates));

    ASSERT_EQ(candidates.size(), size_t(1));
    EXPECT_EQ(candidates.count("oversized"), size_t(0));
    EXPECT_EQ(candidates.count("fits"), size_t(1));
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("oversized"));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("fits"));
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

// Verifies candidate sizing uses the real allocated size (sallocx) rather than the payload (GetDataSize), so
// migrated_bytes shares the same unit as task.max_bytes (RealUsage). With a standalone 1MiB object plus a non-zero
// metadata header, sallocx rounds beyond the payload, so the candidate size must exceed GetDataSize and equal
// Allocator::GetAllocatedSize on the object's base pointer.
TEST_F(RebalanceCandidateProviderTest, TryGetObjectSizeUsesRealSizeNotPayload)
{
    CreateAndAdd("real_size_obj", 1 * MB);

    MemoryRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    RebalanceCandidateProvider::ObjectHeatMap objectHeats;
    DS_ASSERT_OK(SelectCandidates(provider, 2 * MB, 10, candidates, objectHeats));

    ASSERT_EQ(candidates.count("real_size_obj"), size_t(1));
    // sallocx rounds the allocation up beyond the payload, so the accounting size must be greater than dataSize.
    EXPECT_GT(candidates["real_size_obj"], 1 * MB);
    // The accounting size must equal the sallocx real size of the object's base pointer.
    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get("real_size_obj", entry));
    ASSERT_TRUE(entry->RLock(true));
    auto shmUnit = entry->Get()->GetShmUnit();
    ASSERT_NE(shmUnit, nullptr);
    void *ptr = shmUnit->GetPointer();
    entry->RUnlock();
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(candidates["real_size_obj"], allocator->GetAllocatedSize(ptr));
}

// Verifies GetMigratableSize directly: a standalone ShmUnit (no ShmOwner) returns the sallocx real size, while an
// aggregated slice distributed from a ShmOwner returns the distributed slice size (needSize proxy) because sallocx
// is invalid on an interior pointer. Covers the shmOwner_ != nullptr branch that the candidate-provider path does
// not exercise (CreateAndAdd only produces standalone allocations).
TEST_F(RebalanceCandidateProviderTest, GetMigratableSizeDistinguishesStandaloneAndAggregated)
{
    // Standalone: no ShmOwner -> sallocx real size of the base pointer.
    auto standalone = std::make_shared<ShmUnit>();
    DS_ASSERT_OK(standalone->AllocateMemory("", 1 * MB, false));
    EXPECT_EQ(standalone->GetMigratableSize(), allocator->GetAllocatedSize(standalone->GetPointer()));
    EXPECT_GE(standalone->GetMigratableSize(), 1 * MB);  // sallocx >= needSize

    // Aggregated: distribute a slice from a ShmOwner chunk -> returns the slice size, not sallocx.
    auto owner = std::make_shared<ShmOwner>();
    DS_ASSERT_OK(owner->AllocateMemory("", 4 * MB, false));  // backing chunk
    ShmUnit slice;
    constexpr uint64_t sliceSize = 256 * 1024;  // < 1MB, the aggregation threshold
    DS_ASSERT_OK(owner->DistributeMemory(sliceSize, slice));
    EXPECT_EQ(slice.GetMigratableSize(), sliceSize);  // aggregated branch returns the distributed slice size
}

// Heat-eviction variant of the provider fixture: sets the heat flags BEFORE Init so the manager builds the
// heat strategy (strategy is fixed at worker startup), then CreateAndAdd seeds heat nodes instead of clock
// counter nodes. Required because the rebalance_strategy flag selection happens in the factory, not Init.
class HeatProviderTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        objectTable_ = std::make_shared<ObjectTable>();
        allocator = memory::Allocator::Instance();
        allocator->Init(maxMemorySize);
        akSkManager_ = std::make_shared<AkSkManager>(0);

        FLAGS_eviction_strategy = "heat";
        FLAGS_eviction_heat_threshold = 2.0;
        FLAGS_eviction_heat_initial_counter = 2.0;
        FLAGS_eviction_heat_max_counter = 32;
        FLAGS_rebalance_keep_local_copy = true;
        RefreshHeatFactors();

        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, LOCAL_ADDR, MASTER_ADDR,
                                                                     GetTestMetadataRoute());
        globalRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        DS_ASSERT_OK(evictionManager_->Init(globalRefTable_, akSkManager_));
        std::weak_ptr<WorkerOcEvictionManager> weakManager = evictionManager_;
        AsyncResourceReleaser::Instance().Init(objectTable_, [weakManager](const ImmutableString &objectKey) {
            auto manager = weakManager.lock();
            if (manager != nullptr) {
                manager->Erase(objectKey);
            }
        });
    }

    void TearDown() override
    {
        AsyncResourceReleaser::Instance().Shutdown();
        evictionManager_.reset();
        globalRefTable_.reset();
        akSkManager_.reset();
        objectTable_.reset();
        FLAGS_eviction_strategy = "clock";
        FLAGS_rebalance_keep_local_copy = true;
        RefreshHeatFactors();
        allocator->ResetForTest();
        allocator = nullptr;
        CommonTest::TearDown();
    }

protected:
    void CreateAndAdd(const std::string &objectKey, uint64_t dataSize, bool primaryCopy = true)
    {
        DS_ASSERT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE, primaryCopy));
        evictionManager_->Add(objectKey);
    }

    bool EvictionNodeExists(const std::string &objectKey)
    {
        std::vector<EvictionList::Node> nodes;
        EvictionList::Node oldest;
        if (evictionManager_->GetAllObjectsInfo(nodes, oldest).IsError()) {
            return false;
        }
        return std::any_of(nodes.begin(), nodes.end(), [&objectKey](const EvictionList::Node &node) {
            return node.objectKey == objectKey;
        });
    }

    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
};

// Under memory pressure, the Heat provider selects stable primaries from lowest to highest heat.
// Source nodes are removed transactionally when AsyncResourceReleaser erases the exact migrated object version.
TEST_F(HeatProviderTest, HeatProviderSelectsLowestHeatPrimariesAndReleaseErasesNodes)
{
    CreateAndAdd("cold", 10 * MB);  // heat=2 (initial), not hot
    CreateAndAdd("warm", 10 * MB);  // 2 + 3 hits = 5 (hot)
    CreateAndAdd("hot", 10 * MB);   // 2 + 6 hits = 8 (hot)
    for (int i = 0; i < 3; ++i) {
        evictionManager_->OnCacheHit("warm");
    }
    for (int i = 0; i < 6; ++i) {
        evictionManager_->OnCacheHit("hot");
    }

    HeatRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    RebalanceCandidateProvider::ObjectHeatMap objectHeats;
    DS_ASSERT_OK(SelectCandidates(provider, 100 * MB, 10, candidates, objectHeats));
    ASSERT_EQ(candidates.size(), size_t(3));
    EXPECT_EQ(candidates.count("warm"), size_t(1));
    EXPECT_EQ(candidates.count("hot"), size_t(1));
    EXPECT_EQ(candidates.count("cold"), size_t(1));
    EXPECT_DOUBLE_EQ(objectHeats.at("cold"), 2.0);
    EXPECT_DOUBLE_EQ(objectHeats.at("warm"), 5.0);
    EXPECT_DOUBLE_EQ(objectHeats.at("hot"), 8.0);
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("warm"));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("hot"));

    // Simulate the real SPILL flow. The releaser removes the eviction node before objectTable erase while holding the
    // exact old object write lock.
    const std::vector<std::string> migrated{ "cold", "warm", "hot" };
    for (const auto &key : migrated) {
        std::shared_ptr<SafeObjType> entry;
        DS_ASSERT_OK(objectTable_->Get(key, entry));
        DS_ASSERT_OK(AsyncResourceReleaser::Instance().Release(key, (*entry)->GetCreateTime()));
    }
    std::vector<EvictionList::Node> res;
    EvictionList::Node oldest;
    DS_ASSERT_OK(evictionManager_->GetAllObjectsInfo(res, oldest));
    EXPECT_TRUE(res.empty());
}

TEST_F(HeatProviderTest, HeatProviderSkipsObjectLargerThanRoundBudget)
{
    CreateAndAdd("oversized", 20 * MB);
    CreateAndAdd("fits", 5 * MB);
    for (int i = 0; i < 3; ++i) {
        evictionManager_->OnCacheHit("oversized");  // heat=5, considered before fits
    }
    for (int i = 0; i < 4; ++i) {
        evictionManager_->OnCacheHit("fits");  // heat=6
    }

    HeatRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    DS_ASSERT_OK(SelectCandidates(provider, 10 * MB, 10, candidates));

    ASSERT_EQ(candidates.size(), size_t(1));
    EXPECT_EQ(candidates.count("oversized"), size_t(0));
    EXPECT_GE(candidates["fits"], 5 * MB);
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("oversized"));
    EXPECT_TRUE(evictionManager_->IsObjectBeingRebalanced("fits"));
}

TEST_F(HeatProviderTest, TaskSessionContinuesCandidateWindowAcrossBatches)
{
    CreateAndAdd("cold", 5 * MB);
    CreateAndAdd("warm", 5 * MB);
    CreateAndAdd("hot", 5 * MB);
    evictionManager_->OnCacheHit("warm");
    evictionManager_->OnCacheHit("hot");
    evictionManager_->OnCacheHit("hot");

    HeatRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    RebalanceCandidateSession session;
    std::unordered_map<std::string, uint64_t> firstBatch;
    RebalanceCandidateProvider::ObjectHeatMap firstHeats;
    DS_ASSERT_OK(provider.Select(session, 6 * MB, 1, firstBatch, firstHeats));
    ASSERT_EQ(firstBatch.size(), size_t(1));
    const auto firstKey = firstBatch.begin()->first;
    evictionManager_->UnmarkRebalancingObject(firstKey);

    std::unordered_map<std::string, uint64_t> secondBatch;
    RebalanceCandidateProvider::ObjectHeatMap secondHeats;
    DS_ASSERT_OK(provider.Select(session, 6 * MB, 1, secondBatch, secondHeats));
    ASSERT_EQ(secondBatch.size(), size_t(1));
    EXPECT_NE(secondBatch.begin()->first, firstKey);
}

TEST_F(HeatProviderTest, AsyncReleaseRetryEventuallyRemovesEvictionNode)
{
    CreateAndAdd("warm", 10 * MB);
    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get("warm", entry));
    const auto version = (*entry)->GetCreateTime();
    DS_ASSERT_OK(entry->WLock());
    ASSERT_EQ(AsyncResourceReleaser::Instance().Release("warm", version).GetCode(), K_TRY_AGAIN);
    AsyncResourceReleaser::Instance().AddTask("warm", version);
    entry->WUnlock();

    constexpr int maxWaitCount = 100;
    constexpr int waitIntervalMs = 20;
    int waitCount = 0;
    while ((objectTable_->Contains("warm").IsOk() || EvictionNodeExists("warm"))
           && waitCount++ < maxWaitCount) {
        std::this_thread::sleep_for(std::chrono::milliseconds(waitIntervalMs));
    }
    EXPECT_TRUE(objectTable_->Contains("warm").IsError());
    EXPECT_FALSE(EvictionNodeExists("warm"));
}

TEST_F(HeatProviderTest, RecreateAfterReleaseKeepsNewEvictionNode)
{
    CreateAndAdd("same_key", 10 * MB);
    std::shared_ptr<SafeObjType> oldEntry;
    DS_ASSERT_OK(objectTable_->Get("same_key", oldEntry));
    DS_ASSERT_OK(AsyncResourceReleaser::Instance().Release("same_key", (*oldEntry)->GetCreateTime()));

    CreateAndAdd("same_key", 10 * MB);
    EXPECT_TRUE(objectTable_->Contains("same_key").IsOk());
    EXPECT_TRUE(EvictionNodeExists("same_key"));
}

// Under rebalance_keep_local_copy, selection must not erase eviction-list nodes because the source keeps its
// objectTable entry and is demoted to non-primary after migration. The nodes remain available for normal eviction.
TEST_F(HeatProviderTest, SelectionKeepsNodesWhenKeepLocalCopyEnabled)
{
    FLAGS_rebalance_keep_local_copy = true;
    CreateAndAdd("warm", 10 * MB);  // 2 + 3 hits = 5 (hot)
    CreateAndAdd("hot", 10 * MB);   // 2 + 6 hits = 8 (hot)
    for (int i = 0; i < 3; ++i) {
        evictionManager_->OnCacheHit("warm");
    }
    for (int i = 0; i < 6; ++i) {
        evictionManager_->OnCacheHit("hot");
    }

    HeatRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    DS_ASSERT_OK(SelectCandidates(provider, 100 * MB, 10, candidates));
    ASSERT_EQ(candidates.size(), size_t(2));

    std::vector<EvictionList::Node> res;
    EvictionList::Node oldest;
    DS_ASSERT_OK(evictionManager_->GetAllObjectsInfo(res, oldest));
    ASSERT_EQ(res.size(), size_t(2));  // both nodes still present — not erased
}

// Integration guard for issue #864: after a SPILL migration completes (Release), the migrated object must not be
// re-selected as a rebalance candidate. This reproduces the original failure (stale eviction-list entry re-scanned
// every batch, logging "Key not found") and asserts the root-cause fix at the migration source keeps the eviction
// list truthful, so rebalance never sees the gone object.
TEST_F(RebalanceCandidateProviderTest, SelectDoesNotSeeStaleEntryAfterRelease)
{
    // Order-independent: guarantee clean singleton state before Init (a prior test suite may have left it running).
    AsyncResourceReleaser::Instance().Shutdown();
    std::weak_ptr<WorkerOcEvictionManager> weakManager = evictionManager_;
    AsyncResourceReleaser::Instance().Init(objectTable_, [weakManager](const ImmutableString &objectKey) {
        auto manager = weakManager.lock();
        if (manager != nullptr) {
            manager->Erase(objectKey);
        }
    });
    Raii releaserShutdown([]() { AsyncResourceReleaser::Instance().Shutdown(); });

    CreateAndAdd("migrated", 10 * MB);
    CreateAndAdd("alive", 20 * MB);

    // Simulate SPILL migration success for "migrated": Release is the single point where the SPILL success path
    // erases the object; with the fix it also erases the eviction-list entry.
    DS_ASSERT_OK(AsyncResourceReleaser::Instance().Release("migrated", 1));
    DS_ASSERT_NOT_OK(objectTable_->Contains("migrated"));

    MemoryRebalanceCandidateProvider provider(evictionManager_, objectTable_);
    std::unordered_map<std::string, uint64_t> candidates;
    DS_ASSERT_OK(SelectCandidates(provider, 30 * MB, 10, candidates));

    EXPECT_EQ(candidates.count("migrated"), size_t(0));
    EXPECT_GE(candidates["alive"], 20 * MB);

    std::vector<EvictionList::Node> nodes;
    EvictionList::Node oldest;
    DS_ASSERT_OK(evictionManager_->GetAllObjectsInfo(nodes, oldest));
    bool migratedStillPresent = false;
    for (const auto &node : nodes) {
        if (node.objectKey == "migrated") {
            migratedStillPresent = true;
        }
    }
    EXPECT_FALSE(migratedStillPresent) << "stale eviction-list entry for migrated object must be purged at source";
}

class RebalanceExecutorTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        objectTable_ = std::make_shared<ObjectTable>();
        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, LOCAL_ADDR, MASTER_ADDR,
                                                                     GetTestMetadataRoute());
        RebalanceExecutorConfig config{ LOCAL_ADDR,       &metadataRoute_, &membership_, &endpointPolicy_,
                                        &exitRequested_, nullptr,         nullptr,       objectTable_,
                                        evictionManager_, nullptr };
        executor_ = std::make_unique<RebalanceExecutor>(std::move(config));
    }

protected:
    struct ReportRecord {
        master::RebalanceTaskStatusPb status = master::REBALANCE_TASK_INIT;
        uint64_t migratedBytes = 0;
        uint64_t migratedObjects = 0;
        uint64_t failedObjects = 0;
        master::RebalanceFailureSidePb failureSide = master::REBALANCE_FAILURE_UNKNOWN;
        std::string failedReason;
    };

    void InstallHooks(const std::vector<std::unordered_map<std::string, uint64_t>> &batches,
                      const std::vector<RebalanceExecutor::MigrateResult> &results,
                      std::function<void()> migrateSideEffect = nullptr, bool ensureTopology = true)
    {
        cluster::MemberEndpoint target;
        if (ensureTopology && membership_.ResolveByAddress(TARGET_ADDR, target).IsError()) {
            PublishAssignedMasterState(cluster::MemberState::ACTIVE);
        }
        batches_ = batches;
        results_ = results;
        migrateSideEffect_ = std::move(migrateSideEffect);
        selectIndex_ = 0;
        migrateIndex_ = 0;
        reportCount_ = 0;
        reportStatus_ = Status::OK();
        reports_.clear();
        reportThreadIds_.clear();
        reportTraceIds_.clear();
        nextTaskForReport_.Clear();
        executor_->SetTestHooks(
            [this](uint64_t maxBytes, std::unordered_map<std::string, uint64_t> &candidates,
                   RebalanceExecutor::ObjectHeatMap &objectHeats,
                   const std::unordered_set<std::string> &skipKeys) {
                (void)maxBytes;
                objectHeats.clear();
                if (selectIndex_ >= batches_.size()) {
                    return Status(K_NOT_FOUND, "no more test candidates");
                }
                auto batch = batches_[selectIndex_++];
                for (auto it = batch.begin(); it != batch.end();) {
                    if (skipKeys.count(it->first) > 0) {
                        it = batch.erase(it);
                    } else {
                        ++it;
                    }
                }
                if (batch.empty()) {
                    return Status(K_NOT_FOUND, "all candidates were skipped");
                }
                candidates = std::move(batch);
                return Status::OK();
            },
            [this](const master::RebalanceTaskPb &, const HostPort &, const std::vector<std::string> &objectKeys,
                   const RebalanceExecutor::ObjectHeatMap &) {
                migratedObjectKeys_.push_back(objectKeys);
                if (migrateIndex_ >= results_.size()) {
                    RebalanceExecutor::MigrateResult result;
                    result.status = Status(K_RUNTIME_ERROR, "missing test result");
                    return result;
                }
                auto result = results_[migrateIndex_++];
                if (migrateSideEffect_ != nullptr) {
                    migrateSideEffect_();
                }
                return result;
            },
            [this](const master::ReportRebalanceResultReqPb &req, master::ReportRebalanceResultRspPb &rsp) {
                std::lock_guard<std::mutex> lock(mutex_);
                reports_.push_back({ req.status(), req.migrated_bytes(), req.migrated_objects(), req.failed_objects(),
                                     req.failure_side(), req.failed_reason() });
                reportThreadIds_.push_back(std::this_thread::get_id());
                reportTraceIds_.push_back(Trace::Instance().GetTraceID());
                // Simulate master assigning a follow-up batch: copy the pre-set next task into the
                // response (one-shot) so the executor's master-Rsp-driven loop advances a batch.
                if (req.status() == master::REBALANCE_TASK_SUCCEEDED && !nextTaskForReport_.task_id().empty()) {
                    *rsp.mutable_next_rebalance_task() = nextTaskForReport_;
                    nextTaskForReport_.Clear();
                }
                ++reportCount_;
                cv_.notify_all();
                return reportStatus_;
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

    void PublishAssignedMasterState(
        cluster::MemberState state,
        cluster::EndpointAvailability availability = cluster::EndpointAvailability::UNKNOWN,
        cluster::MemberState targetState = cluster::MemberState::ACTIVE)
    {
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = ++topologyVersion_;
        // Transitional members require a matching activeBatch, otherwise
        // TopologySnapshot::Create rejects the snapshot as an unstable topology.
        if (state == cluster::MemberState::FAILED) {
            topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::FAILURE, topologyVersion_ };
        } else if (state == cluster::MemberState::LEAVING || targetState == cluster::MemberState::LEAVING) {
            topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_IN, topologyVersion_ };
        } else if (state == cluster::MemberState::JOINING || targetState == cluster::MemberState::JOINING) {
            topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_OUT, topologyVersion_ };
        }
        topology.members = {
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 'l'), LOCAL_ADDR.ToString() },
                             cluster::MemberState::ACTIVE, { 0 } },
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 'm'), MASTER_ADDR.ToString() }, state, { 1 } },
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 't'), TARGET_ADDR }, targetState,
                             { TARGET_TOPOLOGY_TOKEN } },
        };
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        DS_ASSERT_OK(cluster::TopologySnapshot::Create(
            std::move(topology), static_cast<int64_t>(topologyVersion_), std::string(TOPOLOGY_DIGEST_SIZE, 'a'),
            snapshot));
        cluster::SnapshotUpdateOutcome outcome;
        DS_ASSERT_OK(snapshots_.Publish(std::move(snapshot), outcome));
        if (availability != cluster::EndpointAvailability::UNKNOWN) {
            cluster::MemberEndpoint endpoint;
            DS_ASSERT_OK(membership_.ResolveByAddress(MASTER_ADDR.ToString(), endpoint));
            cluster::EndpointObservation observation{ endpoint.identity, topologyVersion_, availability };
            DS_ASSERT_OK(membership_.UpdateObservation(observation));
        }
    }

    void PublishTopologyWithoutAssignedMaster()
    {
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = ++topologyVersion_;
        topology.members = {
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 'l'), LOCAL_ADDR.ToString() },
                             cluster::MemberState::ACTIVE, { 0 } },
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 't'), TARGET_ADDR },
                             cluster::MemberState::ACTIVE, { TARGET_TOPOLOGY_TOKEN } },
        };
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        DS_ASSERT_OK(cluster::TopologySnapshot::Create(
            std::move(topology), static_cast<int64_t>(topologyVersion_), std::string(TOPOLOGY_DIGEST_SIZE, 'a'),
            snapshot));
        cluster::SnapshotUpdateOutcome outcome;
        DS_ASSERT_OK(snapshots_.Publish(std::move(snapshot), outcome));
    }

    void PublishScaleOutTopology()
    {
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = ++topologyVersion_;
        topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_OUT, topologyVersion_ };
        topology.members = {
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 'l'), LOCAL_ADDR.ToString() },
                             cluster::MemberState::ACTIVE, { 0 } },
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 'm'), MASTER_ADDR.ToString() },
                             cluster::MemberState::ACTIVE, { 1 } },
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 't'), TARGET_ADDR },
                             cluster::MemberState::ACTIVE, { TARGET_TOPOLOGY_TOKEN } },
            cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 'j'), JOINING_ADDR },
                             cluster::MemberState::JOINING, { TARGET_TOPOLOGY_TOKEN + 1 } },
        };
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        DS_ASSERT_OK(cluster::TopologySnapshot::Create(
            std::move(topology), static_cast<int64_t>(topologyVersion_), std::string(TOPOLOGY_DIGEST_SIZE, 'a'),
            snapshot));
        cluster::SnapshotUpdateOutcome outcome;
        DS_ASSERT_OK(snapshots_.Publish(std::move(snapshot), outcome));
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

    cluster::TopologySnapshotState snapshots_;
    cluster::MembershipEndpointView membership_{ snapshots_ };
    worker::MetadataRouteResolver metadataRoute_{ nullptr, worker::MetadataRouteOptions{} };
    ObjectEndpointPolicy endpointPolicy_{ metadataRoute_, membership_ };
    std::atomic<bool> exitRequested_{ false };
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    std::unique_ptr<RebalanceExecutor> executor_;
    std::vector<std::unordered_map<std::string, uint64_t>> batches_;
    std::vector<RebalanceExecutor::MigrateResult> results_;
    std::function<void()> migrateSideEffect_;
    std::vector<std::vector<std::string>> migratedObjectKeys_;
    uint64_t topologyVersion_ = 0;
    size_t selectIndex_ = 0;
    size_t migrateIndex_ = 0;
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t reportCount_ = 0;
    Status reportStatus_;
    std::vector<ReportRecord> reports_;
    std::vector<std::thread::id> reportThreadIds_;
    // When set (non-empty task_id), the reportHook copies this task into the response's
    // next_rebalance_task and clears it (one-shot), simulating master assigning a follow-up batch.
    master::RebalanceTaskPb nextTaskForReport_;
    std::vector<std::string> reportTraceIds_;
};

TEST_F(RebalanceExecutorTest, AbortsWhenAssignedMasterBecomesFailed)
{
    PublishAssignedMasterState(cluster::MemberState::FAILED);
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });

    executor_->Submit(MakeTask("failed-master-task", 10), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_EXPIRED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_CONTROL_PLANE);
    EXPECT_EQ(migrateIndex_, size_t(0));
    EXPECT_NE(reports_[0].failedReason.find("unavailable"), std::string::npos);
}

TEST_F(RebalanceExecutorTest, StopsAtLocalBatchBoundaryWhenTopologyBatchStarts)
{
    InstallHooks({ { { "obj1", 50 } }, { { "obj2", 50 } } },
                 { MakeMigrateResult({ "obj1" }), MakeMigrateResult({ "obj2" }) },
                 [this]() {
                     if (migrateIndex_ == 1) {
                         PublishScaleOutTopology();
                     }
                 });

    executor_->Submit(MakeTask("topology-batch-stop-task", 100), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(reports_[0].migratedBytes, 50u);
    EXPECT_EQ(migrateIndex_, size_t(1));
}

TEST_F(RebalanceExecutorTest, AbortsWhenAssignedMasterIsUnreachable)
{
    PublishAssignedMasterState(cluster::MemberState::ACTIVE, cluster::EndpointAvailability::UNREACHABLE);
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });

    executor_->Submit(MakeTask("unreachable-master-task", 10), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_EXPIRED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_CONTROL_PLANE);
    EXPECT_EQ(migrateIndex_, size_t(0));
    EXPECT_NE(reports_[0].failedReason.find("unavailable"), std::string::npos);
}

TEST_F(RebalanceExecutorTest, AbortsWhenAssignedMasterIsAbsentFromTopology)
{
    PublishTopologyWithoutAssignedMaster();
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });

    executor_->Submit(MakeTask("missing-master-task", 10), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_EXPIRED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_CONTROL_PLANE);
    EXPECT_EQ(migrateIndex_, size_t(0));
    EXPECT_NE(reports_[0].failedReason.find("not found"), std::string::npos);
}

TEST_F(RebalanceExecutorTest, RejectsTargetBeforeTopologySnapshotIsReady)
{
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) }, nullptr, false);

    executor_->Submit(MakeTask("topology-not-ready-task", 10), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_FAILED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_TARGET);
    EXPECT_EQ(reports_[0].migratedBytes, 0u);
    EXPECT_EQ(selectIndex_, 0u);
    EXPECT_EQ(migrateIndex_, 0u);
}

TEST_F(RebalanceExecutorTest, ContinuesWhileAssignedMasterIsActive)
{
    PublishAssignedMasterState(cluster::MemberState::ACTIVE);
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });

    executor_->Submit(MakeTask("active-master-task", 10), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(migrateIndex_, size_t(1));
}

TEST_F(RebalanceExecutorTest, ExpiresWhenAssignedMasterFailsDuringLastBatch)
{
    PublishAssignedMasterState(cluster::MemberState::ACTIVE);
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) },
                 [this] { PublishAssignedMasterState(cluster::MemberState::FAILED); });

    executor_->Submit(MakeTask("master-fails-during-batch", 10), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_EXPIRED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_CONTROL_PLANE);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(10));
    EXPECT_EQ(migrateIndex_, size_t(1));
}

// Verifies the successful executor path: selected objects migrate successfully, the executor reports SUCCEEDED, clears
// its running task state, and releases the rebalancing marks for the batch.
TEST_F(RebalanceExecutorTest, SubmitReportsSucceededAndClearsRunningState)
{
    InstallHooks({ { { "obj1", 10 }, { "obj2", 20 } } }, { MakeMigrateResult({ "obj1", "obj2" }) });
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj1"));
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj2"));

    executor_->Submit(MakeTask("success-task", 30), MASTER_ADDR.ToString());

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

TEST_F(RebalanceExecutorTest, RejectsTaskWhenSourcePolicyEpochIsStale)
{
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });
    auto task = MakeTask("stale-policy-task", 10);
    task.set_source_eviction_policy_epoch(1);

    executor_->Submit(task, MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_FAILED);
    EXPECT_NE(reports_[0].failedReason.find("fence is stale"), std::string::npos);
    EXPECT_EQ(migrateIndex_, size_t(0));
}

TEST_F(RebalanceExecutorTest, NonBlockingPauseReturnsTryAgainWhileTaskDrains)
{
    std::mutex migrationMutex;
    std::condition_variable migrationCv;
    bool migrationEntered = false;
    bool releaseMigration = false;
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "unused" }) });
    executor_->SetTestHooks(
        [this](uint64_t, std::unordered_map<std::string, uint64_t> &candidates,
               RebalanceExecutor::ObjectHeatMap &objectHeats,
               const std::unordered_set<std::string> & /* skipKeys */) {
            objectHeats.clear();
            if (selectIndex_ >= batches_.size()) {
                return Status(K_NOT_FOUND, "no more test candidates");
            }
            candidates = batches_[selectIndex_++];
            return Status::OK();
        },
        [this, &migrationMutex, &migrationCv, &migrationEntered, &releaseMigration](
            const master::RebalanceTaskPb &, const HostPort &, const std::vector<std::string> &objectKeys,
            const RebalanceExecutor::ObjectHeatMap &) {
            std::unique_lock<std::mutex> lock(migrationMutex);
            migrationEntered = true;
            migrationCv.notify_all();
            migrationCv.wait(lock, [&releaseMigration] { return releaseMigration; });
            return MakeMigrateResult(objectKeys);
        },
        [this](const master::ReportRebalanceResultReqPb &req, master::ReportRebalanceResultRspPb &) {
            std::lock_guard<std::mutex> lock(mutex_);
            reports_.push_back({ req.status(), req.migrated_bytes(), req.migrated_objects(), req.failed_objects(),
                                 req.failure_side(), req.failed_reason() });
            ++reportCount_;
            cv_.notify_all();
            return Status::OK();
        });

    executor_->Submit(MakeTask("nonblocking-pause", 10), MASTER_ADDR.ToString());
    {
        std::unique_lock<std::mutex> lock(migrationMutex);
        ASSERT_TRUE(migrationCv.wait_for(lock, std::chrono::seconds(2), [&migrationEntered] {
            return migrationEntered;
        }));
    }
    Timer timer;
    EXPECT_EQ(executor_->PauseAndCheckDrained().GetCode(), K_TRY_AGAIN);
    EXPECT_LT(timer.ElapsedMilliSecond(), 100u);

    {
        std::lock_guard<std::mutex> lock(migrationMutex);
        releaseMigration = true;
    }
    migrationCv.notify_all();
    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    DS_ASSERT_OK(executor_->PauseAndCheckDrained());
    executor_->Resume();
}

TEST_F(RebalanceExecutorTest, FailedTerminalReportReplayOnlyResendsCachedResult)
{
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });
    reportStatus_ = Status(K_RPC_UNAVAILABLE, "injected report failure");
    auto task = MakeTask("replayed-task", 10);

    executor_->Submit(task, MASTER_ADDR.ToString());
    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(migrateIndex_, size_t(1));

    DS_ASSERT_OK(executor_->PauseAndCheckDrained());
    executor_->Submit(task, MASTER_ADDR.ToString());
    ASSERT_TRUE(WaitReports(2));
    executor_->Resume();
    EXPECT_EQ(migrateIndex_, size_t(1));
    ASSERT_EQ(reports_.size(), size_t(2));
    EXPECT_EQ(reports_[1].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(reports_[1].migratedBytes, uint64_t(10));
}

// Verifies that Submit propagates the caller's traceId into the single-task executor pool, so worker-side
// rebalance logs and the downstream ReportRebalanceResult RPC carry the same trace as the master scheduler logs.
TEST_F(RebalanceExecutorTest, SubmitPropagatesCallerTraceIdToExecutorPool)
{
    // InstallHooks defaults ensureTopology=true -> publishes ACTIVE topology, and the success migrate
    // result makes Execute reach ReportResult on the pool thread where the report hook captures the trace.
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });

    const std::string knownTrace = "ut-rebalance;trace-prop";
    {
        TraceGuard guard = Trace::Instance().SetTraceNewID(knownTrace);
        executor_->Submit(MakeTask("trace-prop-task", 10), MASTER_ADDR.ToString());
        ASSERT_TRUE(WaitReports(1));
        ASSERT_TRUE(WaitTaskDone());
    }
    ASSERT_EQ(reports_.size(), size_t(1));
    ASSERT_EQ(reportTraceIds_.size(), size_t(1));
    EXPECT_EQ(reportTraceIds_[0], knownTrace)
        << "executorPool thread must inherit the caller's traceId via Submit propagation";
}

// Verifies the failed executor path: a migration error or failed object causes FAILED to be reported with accurate
// migrated/failed counters, and the executor still clears running state and rebalancing marks.
TEST_F(RebalanceExecutorTest, SubmitReportsFailedAndClearsRunningState)
{
    InstallHooks({ { { "obj1", 10 }, { "obj2", 20 } } },
                 { MakeFailedMigrateResult({ "obj1" }, { "obj2" }, Status(K_RUNTIME_ERROR, "migrate failed")) });
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj1"));
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj2"));

    executor_->Submit(MakeTask("failed-task", 30), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_FAILED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_UNKNOWN);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(10));
    EXPECT_EQ(reports_[0].migratedObjects, uint64_t(1));
    EXPECT_EQ(reports_[0].failedObjects, uint64_t(1));
    EXPECT_NE(reports_[0].failedReason.find("migrate failed"), std::string::npos);
    EXPECT_FALSE(executor_->IsRunningForTest());
    EXPECT_TRUE(executor_->GetRunningTaskIdForTest().empty());
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("obj1"));
    EXPECT_FALSE(evictionManager_->IsObjectBeingRebalanced("obj2"));
}

TEST_F(RebalanceExecutorTest, AttributesStructuredUbFailureToOperatorWorker)
{
    const auto runCase = [this](const std::string &taskId, const HostPort &operatorWorker,
                                master::RebalanceFailureSidePb expectedSide) {
        auto result = MakeFailedMigrateResult({}, { "obj1" }, Status(K_URMA_ERROR, "provider error 4"));
        result.ubFailureDetail.emplace();
        result.ubFailureDetail->set_operator_worker(operatorWorker.ToString());
        InstallHooks({ { { "obj1", 10 } } }, { result });

        executor_->Submit(MakeTask(taskId, 10), MASTER_ADDR.ToString());

        ASSERT_TRUE(WaitReports(1));
        ASSERT_TRUE(WaitTaskDone());
        ASSERT_EQ(reports_.size(), size_t(1));
        EXPECT_EQ(reports_[0].failureSide, expectedSide);
    };
    runCase("source-operator-failure", LOCAL_ADDR, master::REBALANCE_FAILURE_SOURCE);
    HostPort target;
    DS_ASSERT_OK(target.ParseString(TARGET_ADDR));
    runCase("target-operator-failure", target, master::REBALANCE_FAILURE_TARGET);
}

// Verifies multi-batch execution. If one batch does not satisfy task.max_bytes, the executor keeps selecting and
// migrating additional batches until the requested byte target is reached.
TEST_F(RebalanceExecutorTest, SubmitRunsMultipleBatchesUntilTargetBytesReached)
{
    InstallHooks({ { { "obj1", 30 }, { "obj2", 30 } }, { { "obj3", 40 } } },
                 { MakeMigrateResult({ "obj1", "obj2" }), MakeMigrateResult({ "obj3" }) });

    executor_->Submit(MakeTask("multi-batch-task", 100), MASTER_ADDR.ToString());

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

TEST_F(RebalanceExecutorTest, RejectsJoiningTargetBeforeSelectingCandidates)
{
    PublishAssignedMasterState(cluster::MemberState::ACTIVE, cluster::EndpointAvailability::UNKNOWN,
                               cluster::MemberState::JOINING);
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });

    executor_->Submit(MakeTask("joining-target-task", 10), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_FAILED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_TARGET);
    EXPECT_EQ(reports_[0].migratedBytes, 0u);
    EXPECT_EQ(selectIndex_, 0u);
    EXPECT_EQ(migrateIndex_, 0u);
}

TEST_F(RebalanceExecutorTest, StopsBeforeNextBatchWhenTargetStartsLeaving)
{
    PublishAssignedMasterState(cluster::MemberState::ACTIVE);
    InstallHooks({ { { "obj1", 50 } }, { { "obj2", 50 } } },
                 { MakeMigrateResult({ "obj1" }), MakeMigrateResult({ "obj2" }) },
                 [this] {
                     PublishAssignedMasterState(cluster::MemberState::ACTIVE,
                                                cluster::EndpointAvailability::UNKNOWN,
                                                cluster::MemberState::LEAVING);
                 });

    executor_->Submit(MakeTask("target-leaves-mid-task", 100), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_FAILED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_TARGET);
    EXPECT_EQ(reports_[0].migratedBytes, 50u);
    EXPECT_EQ(selectIndex_, 1u);
    EXPECT_EQ(migrateIndex_, 1u);
}

// Exhausting local candidates after a successful batch is a valid partial completion. The migrated data remains
// effective, so the result must not be reported as FAILED and put the source/target pair into cooldown.
TEST_F(RebalanceExecutorTest, SubmitReportsSucceededWhenCandidatesExhaustedAfterPartialMigration)
{
    InstallHooks({ { { "obj1", 30 }, { "obj2", 20 } } }, { MakeMigrateResult({ "obj1", "obj2" }) });

    executor_->Submit(MakeTask("partial-task", 100), MASTER_ADDR.ToString());

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
    task.set_create_time_ms(0);
    task.set_timeout_ms(0);
    task.set_deadline_ms(static_cast<uint64_t>(GetSteadyClockTimeStampMs()) - 1);

    executor_->Submit(task, MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_EXPIRED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_CONTROL_PLANE);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(0));
    EXPECT_EQ(reports_[0].migratedObjects, uint64_t(0));
    EXPECT_EQ(reports_[0].failedObjects, uint64_t(0));
    EXPECT_TRUE(migratedObjectKeys_.empty());
    EXPECT_FALSE(executor_->IsRunningForTest());
    EXPECT_TRUE(executor_->GetRunningTaskIdForTest().empty());
}

// Verifies that worker-side expiry is based on the relative timeout assigned by master. The raw master deadline may be
// smaller than the source worker's steady-clock value on another host, but the task must still execute.
TEST_F(RebalanceExecutorTest, SubmitUsesTimeoutAsSourceLocalDeadline)
{
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });
    auto task = MakeTask("relative-timeout-task", 10);
    task.set_create_time_ms(1);
    task.set_deadline_ms(1);
    task.set_timeout_ms(TASK_TIMEOUT_MS);

    executor_->Submit(task, MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(10));
    EXPECT_EQ(migrateIndex_, size_t(1));
}

// Verifies duplicate-source protection. When the source worker is already running another task, a new task is rejected
// with FAILED and the existing running task id is left intact.
TEST_F(RebalanceExecutorTest, BusySourceReportsFailedWithoutReplacingRunningTask)
{
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });
    executor_->SetRunningForTest(true, "running-task");
    auto submitThreadId = std::this_thread::get_id();

    executor_->Submit(MakeTask("new-task", 10), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_FAILED);
    EXPECT_EQ(reports_[0].failureSide, master::REBALANCE_FAILURE_SOURCE);
    EXPECT_NE(reports_[0].failedReason.find("busy"), std::string::npos);
    ASSERT_EQ(reportThreadIds_.size(), size_t(1));
    EXPECT_NE(reportThreadIds_[0], submitThreadId);
    EXPECT_TRUE(executor_->IsRunningForTest());
    EXPECT_EQ(executor_->GetRunningTaskIdForTest(), "running-task");
}

TEST_F(RebalanceExecutorTest, BusySuccessorReplaysCachedPredecessorTerminalResult)
{
    InstallHooks({}, {});
    auto predecessor = MakeTask("completed-predecessor", 10);
    executor_->CacheTerminalResultForTest(predecessor);
    executor_->SetRunningForTest(true, "running-successor");

    executor_->Submit(predecessor, MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(reports_[0].failedReason.find("busy"), std::string::npos);
    EXPECT_TRUE(executor_->IsRunningForTest());
    EXPECT_EQ(executor_->GetRunningTaskIdForTest(), "running-successor");
}

TEST_F(RebalanceExecutorTest, LegacyTaskWithoutPolicyFenceRemainsAccepted)
{
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });
    auto task = MakeTask("legacy-no-fence", 10);
    task.clear_has_eviction_policy_fence();
    task.clear_source_eviction_policy();
    task.clear_target_eviction_policy();

    executor_->Submit(task, MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);

    const auto fence = RebalanceExecutor::BuildRebalancePolicyFenceForTest(task);
    EXPECT_FALSE(fence.enabled);
    EXPECT_EQ(fence.targetPolicy, 0u);
    EXPECT_EQ(fence.targetEpoch, 0u);
    EXPECT_TRUE(fence.taskId.empty());
}

TEST_F(RebalanceExecutorTest, PolicyFenceIsForwardedToTargetMigration)
{
    auto task = MakeTask("policy-fence", 10);
    task.set_target_eviction_policy(master::EVICTION_POLICY_HEAT);
    task.set_target_eviction_policy_epoch(42);

    const auto fence = RebalanceExecutor::BuildRebalancePolicyFenceForTest(task);
    EXPECT_TRUE(fence.enabled);
    EXPECT_EQ(fence.targetPolicy, static_cast<uint32_t>(master::EVICTION_POLICY_HEAT));
    EXPECT_EQ(fence.targetEpoch, 42u);
    EXPECT_EQ(fence.taskId, "policy-fence");
}

// Verifies that SubmitBusyResult propagates the caller's traceId to the executor pool, mirroring Submit's propagation.
// Reverting the SetRequestContext(nullptr)+ScopedRequestContext in SubmitBusyResult causes this assertion to fail.
TEST_F(RebalanceExecutorTest, SubmitBusyResultPropagatesCallerTraceIdToExecutorPool)
{
    InstallHooks({ { { "obj1", 10 } } }, { MakeMigrateResult({ "obj1" }) });
    executor_->SetRunningForTest(true, "running-task");

    const std::string knownTrace = "ut-busy;trace-prop";
    {
        TraceGuard guard = Trace::Instance().SetTraceNewID(knownTrace);
        executor_->Submit(MakeTask("busy-trace-task", 10), MASTER_ADDR.ToString());
        ASSERT_TRUE(WaitReports(1));
    }
    ASSERT_EQ(reports_.size(), size_t(1));
    ASSERT_EQ(reportTraceIds_.size(), size_t(1));
    EXPECT_EQ(reportTraceIds_[0], knownTrace)
        << "SubmitBusyResult must inherit the caller's traceId via executor pool propagation";
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_FAILED);
    EXPECT_NE(reports_[0].failedReason.find("busy"), std::string::npos);
}

// Regression guard for the master-Rsp-driven multi-batch loop: while the executor processes a
// follow-up batch (assigned via ReportResult's next_rebalance_task), a periodic ResourceReport
// returns the still-active batch task (master's NeedSnapshotForSchedule finds the active task
// and returns it). Submit must recognize it as a duplicate (runningTaskId_ == task_id) and
// ignore it, NOT report it as "busy" — which would falsely fail the in-progress batch and apply
// a 60s cooldown. The fix keeps runningTaskId_ in sync with the current batch inside the loop.
TEST_F(RebalanceExecutorTest, MultiBatchLoopDoesNotReportBusyWhenHeartbeatReturnsActiveTask)
{
    InstallHooks({ { { "obj1", 30 } }, { { "obj2", 40 } } },
                  { MakeMigrateResult({ "obj1" }), MakeMigrateResult({ "obj2" }) });

    // Master assigns a follow-up batch in the first success report's response.
    const auto batch2 = MakeTask("heartbeat-batch2", 40);
    nextTaskForReport_ = batch2;

    // During batch2's migration, simulate the periodic heartbeat returning the active task.
    // migrateIndex_ is incremented to 2 before the side effect fires on batch2.
    migrateSideEffect_ = [this, batch2]() {
        if (migrateIndex_ == 2) {
            executor_->Submit(batch2, MASTER_ADDR.ToString());
        }
    };

    executor_->Submit(MakeTask("heartbeat-batch1", 30), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(2)) << "expected batch1 + batch2 success reports";
    ASSERT_TRUE(WaitTaskDone());

    // If the fix is missing, SubmitBusyResult queued a failure report that runs after the loop.
    // Wait briefly for it; a passing run must NOT see a third report.
    {
        std::unique_lock<std::mutex> lock(mutex_);
        const bool gotSpurious = cv_.wait_for(lock, std::chrono::milliseconds(500),
                                              [this] { return reportCount_ >= 3; });
        EXPECT_FALSE(gotSpurious) << "spurious busy failure report for the active batch detected";
    }
    ASSERT_EQ(reports_.size(), size_t(2));
    for (const auto &r : reports_) {
        EXPECT_EQ(r.status, master::REBALANCE_TASK_SUCCEEDED);
        EXPECT_EQ(r.failedReason.find("busy"), std::string::npos)
            << "false 'busy' failure leaked into reports";
    }
}

TEST_F(RebalanceExecutorTest, SkipIdsDoesNotAbortBatch)
{
    RebalanceExecutor::MigrateResult result;
    result.status = Status::OK();
    result.successIds.emplace(ImmutableString("obj1"));
    result.skipIds.emplace(ImmutableString("obj2"));

    InstallHooks({ { { "obj1", 10 }, { "obj2", 20 } } }, { std::move(result) });
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj1"));
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("obj2"));

    executor_->Submit(MakeTask("skip-task", 30), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(10));
    EXPECT_EQ(reports_[0].migratedObjects, uint64_t(1));
    EXPECT_FALSE(executor_->IsRunningForTest());
}

TEST_F(RebalanceExecutorTest, SkippedObjectDoesNotStarveSubsequentBatch)
{
    // Batch 1: select {good_obj1}, migrate success -> migratedBytes=10
    // Batch 2: select {skip_obj}, migrate all-skip -> K_NOT_FOUND, lastBatchAllSkipped=true
    //          migratedBytes=10 > 0 -> retry (the fix prevents starvation)
    // Batch 3: select {skip_obj, good_obj2} -> hook filters skip_obj (in taskSkippedKeys)
    //          -> {good_obj2} -> migrate success -> migratedBytes=30 -> target reached -> SUCCEEDED
    RebalanceExecutor::MigrateResult result1;
    result1.status = Status::OK();
    result1.successIds.emplace(ImmutableString("good_obj1"));

    RebalanceExecutor::MigrateResult result2;
    result2.status = Status::OK();
    result2.skipIds.emplace(ImmutableString("skip_obj"));

    RebalanceExecutor::MigrateResult result3;
    result3.status = Status::OK();
    result3.successIds.emplace(ImmutableString("good_obj2"));

    InstallHooks(
        { { { "good_obj1", 10 } }, { { "skip_obj", 100 } }, { { "skip_obj", 100 }, { "good_obj2", 20 } } },
        { std::move(result1), std::move(result2), std::move(result3) });
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("good_obj1"));
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("skip_obj"));
    ASSERT_TRUE(evictionManager_->TryMarkRebalancingObject("good_obj2"));

    executor_->Submit(MakeTask("starvation-task", 30), MASTER_ADDR.ToString());

    ASSERT_TRUE(WaitReports(1));
    ASSERT_TRUE(WaitTaskDone());
    ASSERT_EQ(reports_.size(), size_t(1));
    EXPECT_EQ(reports_[0].status, master::REBALANCE_TASK_SUCCEEDED);
    EXPECT_EQ(reports_[0].migratedBytes, uint64_t(30));
    EXPECT_EQ(reports_[0].migratedObjects, uint64_t(2));
    ASSERT_EQ(migratedObjectKeys_.size(), size_t(3))
        << "expected three batches (success + skip-retry + success)";
    ASSERT_EQ(migratedObjectKeys_[2].size(), size_t(1));
    EXPECT_EQ(migratedObjectKeys_[2][0], "good_obj2")
        << "batch 3 should migrate good_obj2 after filtering skip_obj";
    EXPECT_FALSE(executor_->IsRunningForTest());
}

}  // namespace ut
}  // namespace datasystem
