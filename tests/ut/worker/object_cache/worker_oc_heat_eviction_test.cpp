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
 * Description: Unit tests for the heat-based eviction strategy.
 *
 * Two layers:
 *  - HeatEvictionListTest: exercises the EvictionList heat methods (AddHeatNode,
 *    IncrementHeat, DecayAllAndCollect, ReinsertHot) directly with a mock
 *    copy-type resolver. Deterministic, no manager/object table/memory allocation.
 *  - HeatEvictionTest: exercises manager dispatch and periodic maintenance with
 *    FLAGS_eviction_strategy="heat".
 */

#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/object_cache/eviction_policy_common.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/eviction_list.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"

#include <limits>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "ut/common.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/log/log.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "eviction_manager_common.h"
#include "test_metadata_route.h"

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {

namespace {
double HeatOf(EvictionList &list, const std::string &key)
{
    EvictionList::Node node;
    auto rc = list.GetObjectInfo(key, node);
    EXPECT_TRUE(rc.IsOk()) << "GetObjectInfo failed for " << key;
    return node.heat;
}

EvictionList::HeatNodeMetadata ResolvedMetadata(bool isPrimary)
{
    EvictionList::HeatNodeMetadata metadata;
    metadata.resolved = true;
    metadata.isPrimary = isPrimary;
    return metadata;
}
}  // namespace

// ---- Direct EvictionList heat-method tests ----

TEST(EvictionListLayoutTest, ClockResidentNodeExcludesHeatState)
{
    EXPECT_LT(EvictionList::ClockListNodeResidentSizeForTest(), sizeof(EvictionList::Node));
    EXPECT_GT(EvictionList::HeatStateResidentSizeForTest(), size_t(0));
}

class HeatEvictionListTest : public CommonTest {
public:
    EvictionList list;

    EvictionList::HeatMetadataResolver MetadataResolverFor(std::unordered_map<std::string, bool> map)
    {
        // Capture the map by value (moved in): callers pass temporaries, so a by-reference
        // capture would dangle after MetadataResolverFor returns.
        return [map = std::move(map)](const std::string &key) {
            auto it = map.find(key);
            return ResolvedMetadata(it != map.end() && it->second);
        };
    }
};

// AddHeatNode seeds an initial heat and records lastAccess/lastDelay.
TEST_F(HeatEvictionListTest, AddHeatNodeSeedsHeatAndTimestamps)
{
    list.AddHeatNode("k1", 32.0, 1000);
    EvictionList::Node node;
    ASSERT_TRUE(list.GetObjectInfo("k1", node).IsOk());
    EXPECT_DOUBLE_EQ(node.heat, 32.0);
    EXPECT_EQ(node.lastAccessMs, 1000u);
    EXPECT_EQ(node.lastDelayMs, 1000u);
}

// OnAdd (AddHeatNode) on an existing key is a no-op for heat (no double-count).
TEST_F(HeatEvictionListTest, AddHeatNodeOnExistingKeyIsNoop)
{
    list.AddHeatNode("k1", 32.0, 1000);
    list.AddHeatNode("k1", 1.0, 2000);  // should not change heat
    EXPECT_DOUBLE_EQ(HeatOf(list, "k1"), 32.0);
}

TEST_F(HeatEvictionListTest, InsertOrMergeClockSnapshotPreservesLargerCounters)
{
    EvictionList::Node initial("clock-key", static_cast<uint8_t>(1));
    initial.maxCounter = 2;
    bool inserted = false;
    DS_ASSERT_OK(list.InsertOrMerge(initial, EvictionList::StateKind::CLOCK, 0.0,
                                    EvictionList::HeatMergeMode::PRESERVE_MAX, inserted));
    EXPECT_TRUE(inserted);

    EvictionList::Node update("clock-key", static_cast<uint8_t>(4));
    update.maxCounter = 5;
    DS_ASSERT_OK(list.InsertOrMerge(update, EvictionList::StateKind::CLOCK, 0.0,
                                    EvictionList::HeatMergeMode::PRESERVE_MAX, inserted));
    EXPECT_FALSE(inserted);

    EvictionList::Node merged;
    DS_ASSERT_OK(list.GetObjectInfo("clock-key", merged));
    EXPECT_EQ(merged.curCounter, 4);
    EXPECT_EQ(merged.maxCounter, 5);
}

TEST_F(HeatEvictionListTest, ConcurrentClockHitsPreservePerKeyCounters)
{
    constexpr size_t keyCount = 32;
    constexpr uint8_t maxCounter = 100;
    std::vector<std::string> keys;
    keys.reserve(keyCount);
    for (size_t i = 0; i < keyCount; ++i) {
        keys.emplace_back("clock-key-" + std::to_string(i));
        EvictionList::Node snapshot(keys.back(), 0);
        snapshot.maxCounter = maxCounter;
        bool inserted = false;
        DS_ASSERT_OK(list.InsertOrMerge(snapshot, EvictionList::StateKind::CLOCK, 0.0,
                                        EvictionList::HeatMergeMode::PRESERVE_MAX, inserted));
        ASSERT_TRUE(inserted);
    }

    std::atomic<bool> start{ false };
    std::vector<std::thread> threads;
    threads.reserve(keyCount);
    for (const auto &key : keys) {
        threads.emplace_back([this, &start, key]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (uint8_t hit = 0; hit < maxCounter; ++hit) {
                list.Add(key, Q1);
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }

    for (const auto &key : keys) {
        EvictionList::Node node;
        DS_ASSERT_OK(list.GetObjectInfo(key, node));
        EXPECT_EQ(node.curCounter, maxCounter);
        EXPECT_EQ(node.maxCounter, maxCounter);
    }
}

TEST_F(HeatEvictionListTest, ConcurrentClockMergeAndSnapshotPreserveCounterBounds)
{
    EvictionList::Node initial("clock-key", static_cast<uint8_t>(1));
    bool inserted = false;
    DS_ASSERT_OK(list.InsertOrMerge(initial, EvictionList::StateKind::CLOCK, 0.0,
                                    EvictionList::HeatMergeMode::PRESERVE_MAX, inserted));
    ASSERT_TRUE(inserted);

    std::atomic<bool> start{ false };
    std::atomic<bool> invalidSnapshot{ false };
    auto mergeThread = std::thread([this, &start, &invalidSnapshot]() {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (uint16_t counter = 2; counter <= 100; ++counter) {
            EvictionList::Node snapshot("clock-key", static_cast<uint8_t>(counter));
            bool inserted = false;
            if (list.InsertOrMerge(snapshot, EvictionList::StateKind::CLOCK, 0.0,
                                   EvictionList::HeatMergeMode::PRESERVE_MAX, inserted)
                    .IsError()
                || inserted) {
                invalidSnapshot.store(true, std::memory_order_release);
                return;
            }
        }
    });
    auto snapshotThread = std::thread([this, &start, &invalidSnapshot]() {
        start.store(true, std::memory_order_release);
        for (size_t i = 0; i < 1'000; ++i) {
            EvictionList::Node snapshot;
            if (list.GetObjectInfo("clock-key", snapshot).IsError() || snapshot.curCounter > snapshot.maxCounter
                || snapshot.maxCounter > 100) {
                invalidSnapshot.store(true, std::memory_order_release);
                return;
            }
        }
    });
    mergeThread.join();
    snapshotThread.join();

    EXPECT_FALSE(invalidSnapshot.load(std::memory_order_acquire));
    EvictionList::Node merged;
    DS_ASSERT_OK(list.GetObjectInfo("clock-key", merged));
    EXPECT_EQ(merged.curCounter, 100);
    EXPECT_EQ(merged.maxCounter, 100);
}

TEST_F(HeatEvictionListTest, InsertOrMergeHeatSnapshotPreservesHotterNewerState)
{
    EvictionList::Node initial("heat-key", 4.0, 1000, 900);
    bool inserted = false;
    DS_ASSERT_OK(list.InsertOrMerge(initial, EvictionList::StateKind::HEAT, 32.0,
                                    EvictionList::HeatMergeMode::PRESERVE_MAX, inserted));
    EXPECT_TRUE(inserted);

    EvictionList::Node update("heat-key", 8.0, 800, 1200);
    DS_ASSERT_OK(list.InsertOrMerge(update, EvictionList::StateKind::HEAT, 32.0,
                                    EvictionList::HeatMergeMode::PRESERVE_MAX, inserted));
    EXPECT_FALSE(inserted);

    EvictionList::Node merged;
    DS_ASSERT_OK(list.GetObjectInfo("heat-key", merged));
    EXPECT_DOUBLE_EQ(merged.heat, 8.0);
    EXPECT_EQ(merged.lastAccessMs, 1000u);
    EXPECT_EQ(merged.lastDelayMs, 1200u);
    EXPECT_NE(merged.generation, 0u);
}

TEST_F(HeatEvictionListTest, InsertOrMergeClockToHeatAddsHeatAndCapsAtMaximum)
{
    constexpr double maxHeat = 32.0;
    EvictionList::Node initial("heat-key", 20.0, 1000, 900);
    bool inserted = false;
    DS_ASSERT_OK(list.InsertOrMerge(initial, EvictionList::StateKind::HEAT, maxHeat,
                                    EvictionList::HeatMergeMode::ADD_CAPPED, inserted));
    EXPECT_TRUE(inserted);

    EvictionList::Node firstSource("heat-key", 8.0, 800, 1200);
    DS_ASSERT_OK(list.InsertOrMerge(firstSource, EvictionList::StateKind::HEAT, maxHeat,
                                    EvictionList::HeatMergeMode::ADD_CAPPED, inserted));
    EXPECT_FALSE(inserted);
    EXPECT_DOUBLE_EQ(HeatOf(list, "heat-key"), 28.0);

    EvictionList::Node secondSource("heat-key", 10.0, 1300, 1100);
    DS_ASSERT_OK(list.InsertOrMerge(secondSource, EvictionList::StateKind::HEAT, maxHeat,
                                    EvictionList::HeatMergeMode::ADD_CAPPED, inserted));
    EXPECT_FALSE(inserted);

    EvictionList::Node merged;
    DS_ASSERT_OK(list.GetObjectInfo("heat-key", merged));
    EXPECT_DOUBLE_EQ(merged.heat, maxHeat);
    EXPECT_EQ(merged.lastAccessMs, 1300u);
    EXPECT_EQ(merged.lastDelayMs, 1200u);
}

TEST_F(HeatEvictionListTest, InsertOrMergeRejectsMismatchedTargetState)
{
    list.AddHeatNode("mixed-key", 2.0, 1000);
    EvictionList::Node clock("mixed-key", static_cast<uint8_t>(1));
    bool inserted = false;
    EXPECT_EQ(list
                  .InsertOrMerge(clock, EvictionList::StateKind::CLOCK, 0.0,
                                 EvictionList::HeatMergeMode::PRESERVE_MAX, inserted)
                  .GetCode(),
              K_RUNTIME_ERROR);
    EXPECT_FALSE(inserted);
    EXPECT_DOUBLE_EQ(HeatOf(list, "mixed-key"), 2.0);
}

TEST_F(HeatEvictionListTest, MembershipAuditChecksPolicyState)
{
    bool inserted = false;
    DS_ASSERT_OK(list.InsertOrMerge(EvictionList::Node("clock", static_cast<uint8_t>(1)),
                                    EvictionList::StateKind::CLOCK, 0.0,
                                    EvictionList::HeatMergeMode::PRESERVE_MAX, inserted));
    DS_ASSERT_OK(list.ValidateMembership(EvictionList::StateKind::CLOCK));
    EXPECT_EQ(list.ValidateMembership(EvictionList::StateKind::HEAT).GetCode(), K_RUNTIME_ERROR);

    DS_ASSERT_OK(list.Erase("clock"));
    DS_ASSERT_OK(list.InsertOrMerge(EvictionList::Node("heat", 4.0, 100, 100),
                                    EvictionList::StateKind::HEAT, 32.0,
                                    EvictionList::HeatMergeMode::PRESERVE_MAX, inserted));
    DS_ASSERT_OK(list.ValidateMembership(EvictionList::StateKind::HEAT));
}

TEST_F(HeatEvictionListTest, ApplyMigratedHeatRestoresNewNodeAndMergesExistingReplica)
{
    list.AddHeatNode("new-target", 2.0, 1000);
    DS_ASSERT_OK(list.ApplyMigratedHeat("new-target", 0.5, 32.0, 2000, false));
    EvictionList::Node node;
    DS_ASSERT_OK(list.GetObjectInfo("new-target", node));
    EXPECT_DOUBLE_EQ(node.heat, 0.5);
    EXPECT_EQ(node.lastAccessMs, 2000u);
    EXPECT_EQ(node.lastDelayMs, 2000u);

    list.AddHeatNode("existing-replica", 12.0, 1000);
    DS_ASSERT_OK(list.ApplyMigratedHeat("existing-replica", 8.0, 32.0, 2000, true));
    EXPECT_DOUBLE_EQ(HeatOf(list, "existing-replica"), 12.0);
    DS_ASSERT_OK(list.ApplyMigratedHeat("existing-replica", 40.0, 32.0, 3000, true));
    EXPECT_DOUBLE_EQ(HeatOf(list, "existing-replica"), 32.0);
}

TEST_F(HeatEvictionListTest, ApplyMigratedHeatRejectsNonFiniteOrNegativeValues)
{
    list.AddHeatNode("k1", 2.0, 1000);
    EXPECT_EQ(list.ApplyMigratedHeat("k1", std::numeric_limits<double>::quiet_NaN(), 32.0, 2000, false).GetCode(),
              K_INVALID);
    EXPECT_EQ(list.ApplyMigratedHeat("k1", -1.0, 32.0, 2000, false).GetCode(), K_INVALID);
    EXPECT_DOUBLE_EQ(HeatOf(list, "k1"), 2.0);
}

// IncrementHeat grows heat by 1 per hit, capped at cap, and refreshes lastAccess.
TEST_F(HeatEvictionListTest, IncrementHeatCapsAtMax)
{
    constexpr double cap = 32.0;
    list.AddHeatNode("k1", 0.0, 1000);
    for (int i = 0; i < 40; ++i) {
        list.IncrementHeat("k1", 1.0, cap, 1000 + i);
    }
    EXPECT_DOUBLE_EQ(HeatOf(list, "k1"), cap);
    EvictionList::Node node;
    ASSERT_TRUE(list.GetObjectInfo("k1", node).IsOk());
    EXPECT_EQ(node.lastAccessMs, 1039u);  // last hit timestamp
}

// IncrementHeat on an unknown key inserts a fresh hot node at cap.
TEST_F(HeatEvictionListTest, IncrementHeatOnUnknownKeyInsertsHot)
{
    list.IncrementHeat("k1", 1.0, 32.0, 5000);
    EXPECT_DOUBLE_EQ(HeatOf(list, "k1"), 32.0);
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesReturnsBoundedOrderAndInvalidatesChangedSnapshot)
{
    list.AddHeatNode("warm", 5.0, 100);
    list.AddHeatNode("cold", 2.0, 200);
    list.AddHeatNode("hot", 10.0, 300);
    list.AddHeatNode("coldest", 1.0, 400);

    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(3.0, 3, candidates));
    ASSERT_EQ(candidates.size(), 3u);
    EXPECT_EQ(candidates[0].objectKey, "coldest");
    EXPECT_EQ(candidates[1].objectKey, "cold");
    EXPECT_EQ(candidates[2].objectKey, "warm");
    EXPECT_TRUE(list.IsHeatSnapshotCurrent(candidates[0]));

    list.AddHeatNode("unrelated", 0.5, 450);
    EXPECT_TRUE(list.IsHeatSnapshotCurrent(candidates[0]));

    list.IncrementHeat("coldest", 1.0, 32.0, 500);
    EXPECT_FALSE(list.IsHeatSnapshotCurrent(candidates[0]));
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesOrdersRecentAccessLastWithoutBlockingProgress)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("older", 2.0, now - 2'000);
    list.AddHeatNode("recent-colder", 1.0, now);

    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(3.0, 8, candidates, 1'000));
    ASSERT_EQ(candidates.size(), 2u);
    EXPECT_EQ(candidates[0].objectKey, "older");
    EXPECT_EQ(candidates[1].objectKey, "recent-colder");

    EvictionList allRecent;
    allRecent.AddHeatNode("recent-a", 2.0, now);
    allRecent.AddHeatNode("recent-b", 1.0, now);
    DS_ASSERT_OK(allRecent.GetHeatCandidates(3.0, 8, candidates, 1'000));
    ASSERT_EQ(candidates.size(), 2u);
    EXPECT_EQ(candidates[0].objectKey, "recent-b");
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesReturnsBoundedUnprotectedSubTwoPool)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("warm", 3.0, now - 4'000);
    list.AddHeatNode("first-cold", 1.5, now - 3'000);
    list.AddHeatNode("colder-but-later", 0.5, now - 5'000);

    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(2.0, 256, candidates, 1'000, 2.0));
    ASSERT_EQ(candidates.size(), 2u);
    EXPECT_EQ(candidates[0].objectKey, "colder-but-later");
    EXPECT_EQ(candidates[1].objectKey, "first-cold");
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesKeepsColdestObjectsFromBoundedScanWindow)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    // Eviction-list insertion is newest-first. Put the globally colder objects at the back so stopping as soon as the
    // output batch fills would retain the wrong pair.
    list.AddHeatNode("coldest", 0.25, now - 5'000);
    list.AddHeatNode("second-coldest", 0.5, now - 4'000);
    list.AddHeatNode("first-seen-warm", 1.5, now - 3'000);
    list.AddHeatNode("first-seen-colder", 1.0, now - 2'000);

    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(2.0, 2, candidates, 1'000, 2.0));
    ASSERT_EQ(candidates.size(), 2u);
    EXPECT_EQ(candidates[0].objectKey, "coldest");
    EXPECT_EQ(candidates[1].objectKey, "second-coldest");
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesKeepsExactOrderingAtOrAboveTwo)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("hot-newer", 5.0, now - 2'000);
    list.AddHeatNode("hot-older", 5.0, now - 4'000);
    list.AddHeatNode("warm", 3.0, now - 3'000);

    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(2.0, 256, candidates, 1'000, 2.0));
    ASSERT_EQ(candidates.size(), 3u);
    EXPECT_EQ(candidates[0].objectKey, "warm");
    EXPECT_EQ(candidates[1].objectKey, "hot-older");
    EXPECT_EQ(candidates[2].objectKey, "hot-newer");
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesDoesNotFastEvictRecentSubTwoObject)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("recent-cold", 1.0, now);
    list.AddHeatNode("older-warm", 3.0, now - 2'000);

    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(2.0, 256, candidates, 1'000, 2.0));
    ASSERT_EQ(candidates.size(), 2u);
    EXPECT_EQ(candidates[0].objectKey, "older-warm");
    EXPECT_EQ(candidates[1].objectKey, "recent-cold");
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesBoundsScanWithoutImmediateCandidate)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    // Insertion is newest-first. The globally coldest object is deliberately beyond the 8 * maxCount eviction scan
    // window. A finite immediate threshold must still cap traversal when no object is below that threshold.
    list.AddHeatNode("outside-window", 2.0, now - 10'000);
    for (size_t i = 0; i < 16; ++i) {
        list.AddHeatNode("window-" + std::to_string(i), 3.0 + static_cast<double>(i), now - i - 1);
    }

    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(2.0, 2, candidates, 0, 2.0));
    ASSERT_EQ(candidates.size(), 2u);
    EXPECT_NE(candidates[0].objectKey, "outside-window");
    EXPECT_NE(candidates[1].objectKey, "outside-window");
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesBoundsScanWhenWholeWindowIsRecent)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    // The cold object is beyond the 8 * maxCount window. Every preceding object is recent, so the traversal bound
    // must not depend on first seeing an unprotected object.
    list.AddHeatNode("outside-window", 0.5, now - 10'000);
    for (size_t i = 0; i < 16; ++i) {
        list.AddHeatNode("recent-" + std::to_string(i), 3.0 + static_cast<double>(i), now);
    }

    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(2.0, 2, candidates, 1'000, 2.0));
    ASSERT_EQ(candidates.size(), 2u);
    EXPECT_NE(candidates[0].objectKey, "outside-window");
    EXPECT_NE(candidates[1].objectKey, "outside-window");
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesRanksResolvedWeakHeatByDensity)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("small-colder", 0.5, now - 4'000);
    list.AddHeatNode("large-cold", 1.5, now - 3'000);
    list.AddHeatNode("orphan", 0.25, now - 5'000);
    list.AddHeatNode("strong-hot", 5.0, now - 6'000);

    const std::unordered_map<std::string, uint64_t> sizes{ { "small-colder", 1 * KB },
                                                           { "large-cold", 64 * KB },
                                                           { "strong-hot", 128 * KB } };
    const auto sizeResolver = [&sizes](const std::string &objectKey, uint64_t &size) {
        const auto found = sizes.find(objectKey);
        if (found == sizes.end()) {
            size = 0;
            return false;
        }
        size = found->second;
        return true;
    };
    EvictionList::HeatCandidateOptions options;
    options.recentAccessProtectionMs = 1'000;
    options.exactRankingThreshold = 4.0;
    options.sizeResolver = sizeResolver;
    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(2.0, 8, candidates, options));

    ASSERT_EQ(candidates.size(), 4u);
    EXPECT_EQ(candidates[0].objectKey, "large-cold");
    EXPECT_EQ(candidates[1].objectKey, "small-colder");
    EXPECT_EQ(candidates[2].objectKey, "orphan");
    EXPECT_EQ(candidates[3].objectKey, "strong-hot");
}

TEST_F(HeatEvictionListTest, GetHeatCandidatesKeepsRecentProtectionAheadOfDensity)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("older-small", 1.0, now - 2'000);
    list.AddHeatNode("recent-large", 1.0, now);
    const auto sizeResolver = [](const std::string &objectKey, uint64_t &size) {
        size = objectKey == "recent-large" ? 64 * KB : 1 * KB;
        return true;
    };
    EvictionList::HeatCandidateOptions options;
    options.recentAccessProtectionMs = 1'000;
    options.exactRankingThreshold = 4.0;
    options.sizeResolver = sizeResolver;
    std::vector<EvictionList::Node> candidates;
    DS_ASSERT_OK(list.GetHeatCandidates(2.0, 8, candidates, options));

    ASSERT_EQ(candidates.size(), 2u);
    EXPECT_EQ(candidates[0].objectKey, "older-small");
    EXPECT_EQ(candidates[1].objectKey, "recent-large");
}

TEST_F(HeatEvictionListTest, ConcurrentHeatUpdatesUseExplicitAtomicSnapshots)
{
    list.AddHeatNode("k1", 0.0, 100);
    constexpr int updateCount = 1000;
    std::atomic<bool> start{ false };
    std::thread updater([this, &start]() {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int i = 0; i < updateCount; ++i) {
            list.IncrementHeat("k1", 1.0, static_cast<double>(updateCount), static_cast<uint64_t>(i + 101));
        }
    });

    start.store(true, std::memory_order_release);
    for (int i = 0; i < updateCount; ++i) {
        EvictionList::Node snapshot;
        DS_ASSERT_OK(list.GetObjectInfo("k1", snapshot));
        EXPECT_GE(snapshot.heat, 0.0);
        EXPECT_LE(snapshot.heat, static_cast<double>(updateCount));
    }
    updater.join();
    EXPECT_DOUBLE_EQ(HeatOf(list, "k1"), static_cast<double>(updateCount));
}

TEST_F(HeatEvictionListTest, ConcurrentSameKeyHitsDoNotLoseIncrements)
{
    constexpr int threadCount = 32;
    constexpr int hitsPerThread = 100;
    constexpr double cap = static_cast<double>(threadCount * hitsPerThread);
    list.AddHeatNode("hot-key", 0.0, 100);
    std::atomic<bool> start{ false };
    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (int thread = 0; thread < threadCount; ++thread) {
        threads.emplace_back([this, &start, thread, cap, hitsPerThread]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int hit = 0; hit < hitsPerThread; ++hit) {
                list.IncrementHeat("hot-key", 1.0, cap, static_cast<uint64_t>(thread * hitsPerThread + hit + 101));
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_DOUBLE_EQ(HeatOf(list, "hot-key"), cap);
    EvictionList::Node node;
    DS_ASSERT_OK(list.GetObjectInfo("hot-key", node));
    EXPECT_EQ(node.lastAccessMs, static_cast<uint64_t>(threadCount * hitsPerThread + 100));
}

// DecayAllAndCollect halves heat over one half-life and refreshes lastDelayMs.
TEST_F(HeatEvictionListTest, DecayAllHalvesOverOneHalfLife)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    const uint64_t past = now - 60000;  // 60s ago
    list.AddHeatNode("k1", 32.0, past);
    auto resolver = MetadataResolverFor({ { "k1", true } });
    (void)list.DecayAllAndCollect(60.0, 30.0, -std::numeric_limits<double>::infinity(),
                                  std::numeric_limits<double>::infinity(), resolver);
    double h = HeatOf(list, "k1");
    EXPECT_NEAR(h, 16.0, 2.0);
    EvictionList::Node node;
    ASSERT_TRUE(list.GetObjectInfo("k1", node).IsOk());
    EXPECT_GE(node.lastDelayMs, now);  // lastDelayMs refreshed to ~now
}

// Local copies decay faster than primary copies under different half-lives.
TEST_F(HeatEvictionListTest, DecayAllLocalDecaysFasterThanPrimary)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    const uint64_t past = now - 120000;  // 120s ago
    list.AddHeatNode("primary", 32.0, past);
    list.AddHeatNode("local", 32.0, past);
    auto resolver = MetadataResolverFor({ { "primary", true }, { "local", false } });
    (void)list.DecayAllAndCollect(120.0, 30.0, -std::numeric_limits<double>::infinity(),
                                  std::numeric_limits<double>::infinity(), resolver);
    double hp = HeatOf(list, "primary");
    double hl = HeatOf(list, "local");
    EXPECT_NEAR(hp, 16.0, 2.0);
    EXPECT_NEAR(hl, 2.0, 1.0);
    EXPECT_LT(hl, hp);
}

// A key can be erased and recreated while decay resolves its copy type outside the list lock. The decay snapshot
// belongs to the old node and must never be applied to the newly-created node with the same key.
TEST_F(HeatEvictionListTest, DecayAllSkipsRecreatedNodeWithSameKey)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("k1", 32.0, now - 60000);

    bool recreated = false;
    (void)list.DecayAllAndCollect(60.0, 30.0, -std::numeric_limits<double>::infinity(),
                                  std::numeric_limits<double>::infinity(),
                                  [this, now, &recreated](const std::string &key) {
                                      if (!recreated) {
                                          recreated = true;
                                          DS_EXPECT_OK(list.Erase(key));
                                          list.AddHeatNode(key, 2.0, now);
                                      }
                                      return ResolvedMetadata(true);
                                  });

    EXPECT_DOUBLE_EQ(HeatOf(list, "k1"), 2.0);
}

// ReinsertHot is an absolute restore, not a cache-hit delta. A decay pass that snapshotted the old heat must not
// reinterpret the restore as additive hits and partially decay it.
TEST_F(HeatEvictionListTest, DecayAllDoesNotOverwriteConcurrentReinsertHot)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("k1", 10.0, now - 60000);

    bool reinserted = false;
    (void)list.DecayAllAndCollect(60.0, 30.0, -std::numeric_limits<double>::infinity(),
                                  std::numeric_limits<double>::infinity(),
                                  [this, now, &reinserted](const std::string &key) {
                                      if (!reinserted) {
                                          reinserted = true;
                                          list.ReinsertHot(key, 32.0, now);
                                      }
                                      return ResolvedMetadata(true);
                                  });

    EXPECT_DOUBLE_EQ(HeatOf(list, "k1"), 32.0);
}

// A cache hit after the decay snapshot but before its CAS apply is an additive update and must survive the decay.
TEST_F(HeatEvictionListTest, DecayAllPreservesHitDuringResolverWindow)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("k1", 10.0, now - 60000);

    bool hitInjected = false;
    (void)list.DecayAllAndCollect(60.0, 30.0, -std::numeric_limits<double>::infinity(),
                                  std::numeric_limits<double>::infinity(),
                                  [this, now, &hitInjected](const std::string &key) {
                                      if (!hitInjected) {
                                          hitInjected = true;
                                          list.IncrementHeat(key, 1.0, 32.0, now);
                                      }
                                      return ResolvedMetadata(true);
                                  });

    EXPECT_NEAR(HeatOf(list, "k1"), 6.0, 0.05);
}

TEST_F(HeatEvictionListTest, DecayAllSkipsUnresolvedMetadata)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    list.AddHeatNode("busy-primary", 32.0, now - 60'000);

    const auto stats = list.DecayAllAndCollect(
        60.0, 30.0, 2.0, 4.0, [](const std::string &) { return EvictionList::HeatNodeMetadata{}; });

    EXPECT_DOUBLE_EQ(HeatOf(list, "busy-primary"), 32.0);
    EXPECT_EQ(stats.scanned, 0u);
    EXPECT_EQ(stats.applied, 0u);
}

// ReinsertHot restores selected heat without discarding a concurrent hit increase.
TEST_F(HeatEvictionListTest, ReinsertHotRestoresSelectedHeatAndPreservesConcurrentIncrease)
{
    list.AddHeatNode("k1", 8.0, 1000);
    list.ReinsertHot("k1", 4.0, 2000);
    EXPECT_DOUBLE_EQ(HeatOf(list, "k1"), 8.0);

    list.ReinsertHot("missing", 4.0, 2000);
    EXPECT_DOUBLE_EQ(HeatOf(list, "missing"), 4.0);
}

TEST_F(HeatEvictionListTest, ExtractAndRestorePreservesExactHeatNode)
{
    list.AddHeatNode("k1", 7.0, 1000);
    list.IncrementHeat("k1", 1.0, 32.0, 1500);
    EvictionList::Node removed;
    DS_ASSERT_OK(list.Extract("k1", removed));
    EXPECT_FALSE(list.Exist("k1"));

    list.Restore(removed);
    EvictionList::Node restored;
    DS_ASSERT_OK(list.GetObjectInfo("k1", restored));
    EXPECT_DOUBLE_EQ(restored.heat, removed.heat);
    EXPECT_EQ(restored.lastAccessMs, removed.lastAccessMs);
    EXPECT_EQ(restored.lastDelayMs, removed.lastDelayMs);
    EXPECT_EQ(restored.generation, removed.generation);
    EXPECT_EQ(restored.heatUpdateSeq, removed.heatUpdateSeq);
}

// ---- Manager dispatch tests (FLAGS_eviction_strategy="heat") ----

class HeatEvictionTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        const uint64_t memSize = 1024ul * 1024ul * 1024ul;
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(memSize);

        FLAGS_eviction_strategy = "heat";
        FLAGS_eviction_heat_half_life_primary_s = 120.0;
        FLAGS_eviction_heat_half_life_local_s = 30.0;
        FLAGS_eviction_heat_threshold = 2.0;
        FLAGS_eviction_heat_max_counter = 32;
        FLAGS_eviction_heat_initial_counter = 2.0;
        RefreshHeatFactors();

        objectTable_ = std::make_shared<ObjectTable>();
        gRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        akSkManager_ = std::make_shared<AkSkManager>(0);
        DS_ASSERT_OK(akSkManager_->SetClientAkSk(clientId_, clientKey_));
        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, workerAddr_, workerAddr_,
                                                                     GetTestMetadataRoute());
        DS_ASSERT_OK(evictionManager_->Init(gRefTable_, akSkManager_));
    }

    void TearDown() override
    {
        evictionManager_.reset();
        objectTable_.reset();
        gRefTable_.reset();
        akSkManager_.reset();
        allocator->ResetForTest();
        allocator = nullptr;
        FLAGS_eviction_strategy = "clock";
        RefreshHeatFactors();
    }

    double HeatOf(const std::string &key)
    {
        EvictionList::Node node;
        if (evictionManager_->GetObjectInfo(key, node).IsError()) {
            return -1.0;
        }
        return node.heat;
    }
    void SeedHeat(const std::string &key, double heat, uint64_t nowMs)
    {
        if (!objectTable_->Contains(key)) {
            DS_ASSERT_OK(CreateObject(key, 1, WriteMode::NONE_L2_CACHE_EVICT));
        }
        evictionManager_->AddHeatNodeForTest(key, heat, nowMs);
    }
    void SeedOrphanHeat(const std::string &key, double heat, uint64_t nowMs)
    {
        evictionManager_->AddHeatNodeForTest(key, heat, nowMs);
    }
    EvictionList::Node NodeOf(const std::string &key)
    {
        EvictionList::Node node;
        EXPECT_TRUE(evictionManager_->GetObjectInfo(key, node).IsOk());
        return node;
    }
    WorkerOcEvictionManager::CopyWatermarkStats CopyWatermarkStats()
    {
        WorkerOcEvictionManager::CopyWatermarkStats stats;
        EXPECT_TRUE(evictionManager_->CollectCopyWatermarkStatsForTest(stats).IsOk());
        return stats;
    }
    void RefreshCopyWatermarkSnapshot()
    {
        evictionManager_->RefreshCopyWatermarkSnapshot();
    }
    void NotifyCopyWatermarkObserver()
    {
        evictionManager_->NotifyCopyWatermarkObserverForTest();
    }
    void MarkPrimaryEndLifeTaskActive(const std::string &key, uint64_t version)
    {
        evictionManager_->MarkPrimaryEndLifeTaskActiveForTest(key, version);
    }
    void FinishPrimaryEndLifeTaskAndWorker(const std::string &key, uint64_t version)
    {
        evictionManager_->FinishPrimaryEndLifeTaskAndWorkerForTest(key, version);
    }
    void HoldStableRouteReader(const std::function<void()> &callback)
    {
        evictionManager_->HoldStableRouteReaderForTest(callback);
    }
    Status MaintainHeat()
    {
        WorkerOcEvictionManager::CopyWatermarkStats stats;
        return evictionManager_->MaintainHeatAndCollectHotPrimaryStats(stats);
    }
    WorkerOcEvictionManager::PolicyUpdatePhase PolicyPhase() const
    {
        return evictionManager_->GetPolicyStateSnapshot().phase;
    }

    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    HostPort workerAddr_{ "localhost", 18481 };
    std::string clientId_ = "test-client-id";
    std::string clientKey_ = "test-client-key";
    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> gRefTable_;
};

// OnCacheHit dispatches to the heat strategy and increments heat by 1 (capped).
TEST_F(HeatEvictionTest, OnCacheHitIncrementsHeat)
{
    // Seed a low-heat node directly (first-publish would set heat=cap, hiding increments).
    SeedHeat("k1", 5.0, GetSteadyClockTimeStampMs());
    ASSERT_DOUBLE_EQ(HeatOf("k1"), 5.0);
    evictionManager_->OnCacheHit("k1");
    EXPECT_DOUBLE_EQ(HeatOf("k1"), 6.0);
    // Repeated hits saturate at the cap.
    for (int i = 0; i < 100; ++i) {
        evictionManager_->OnCacheHit("k1");
    }
    EXPECT_DOUBLE_EQ(HeatOf("k1"), 32.0);
}

TEST_F(HeatEvictionTest, RefillCountsCurrentGetAndProtectsAgainstUntouchedColdObject)
{
    evictionManager_->Add("untouched-cold");
    evictionManager_->OnRefill("refilled");
    EXPECT_DOUBLE_EQ(HeatOf("untouched-cold"), 2.0);
    EXPECT_DOUBLE_EQ(HeatOf("refilled"), 3.0);
    evictionManager_->OnRefill("refilled");
    EXPECT_DOUBLE_EQ(HeatOf("refilled"), 4.0);

    EvictionList isolatedList;
    HeatEvictionStrategy strategy(isolatedList, objectTable_, GetCurrentHeatPolicyConfig());
    strategy.OnAdd("cold");
    strategy.OnRefill("refill");
    EvictionRoundState round;
    EvictionCandidate candidate;
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    EXPECT_EQ(candidate.objectKey, "cold");
}

TEST_F(HeatEvictionTest, AccessHeatCreditIsNormalizedByAllocatorBytes)
{
    EvictionList isolatedList;
    HeatEvictionStrategy strategy(isolatedList, objectTable_, GetCurrentHeatPolicyConfig());
    strategy.OnRefill("small", 1024);
    strategy.OnRefill("large", 128 * KB);

    EvictionList::Node small;
    EvictionList::Node large;
    DS_ASSERT_OK(isolatedList.GetObjectInfo("small", small));
    DS_ASSERT_OK(isolatedList.GetObjectInfo("large", large));
    EXPECT_DOUBLE_EQ(small.heat, 3.0);
    EXPECT_DOUBLE_EQ(large.heat, 2.03125);

    for (int i = 0; i < 8; ++i) {
        strategy.OnCacheHit("large", 128 * KB);
    }
    DS_ASSERT_OK(isolatedList.GetObjectInfo("large", large));
    EXPECT_DOUBLE_EQ(large.heat, 2.28125);
}

TEST_F(HeatEvictionTest, SizeAwareSelectionPrefersLowerHeatPerByteWithinColdPool)
{
    constexpr uint64_t smallPayloadSize = 1 * KB;
    constexpr uint64_t largePayloadSize = 64 * KB;
    DS_ASSERT_OK(CreateObject("small-colder", smallPayloadSize, WriteMode::NONE_L2_CACHE_EVICT));
    DS_ASSERT_OK(CreateObject("large-cold", largePayloadSize, WriteMode::NONE_L2_CACHE_EVICT));

    const uint64_t now = GetSteadyClockTimeStampMs();
    EvictionList isolatedList;
    // Raw heat and age both prefer small-colder. Its heat density is nevertheless much higher than the 64 KiB
    // object's, so a request-hit-per-byte policy must release large-cold first.
    isolatedList.AddHeatNode("large-cold", 1.5, now - 3'000);
    isolatedList.AddHeatNode("small-colder", 0.5, now - 4'000);
    HeatEvictionStrategy strategy(isolatedList, objectTable_, GetCurrentHeatPolicyConfig());

    EvictionRoundState round;
    EvictionCandidate candidate;
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    EXPECT_EQ(candidate.objectKey, "large-cold");
}

TEST_F(HeatEvictionTest, SizeAwareSelectionKeepsRecentProtectionAheadOfDensity)
{
    DS_ASSERT_OK(CreateObject("older-small", 1 * KB, WriteMode::NONE_L2_CACHE_EVICT));
    DS_ASSERT_OK(CreateObject("recent-large", 64 * KB, WriteMode::NONE_L2_CACHE_EVICT));

    const uint64_t now = GetSteadyClockTimeStampMs();
    EvictionList isolatedList;
    isolatedList.AddHeatNode("older-small", 1.0, now - 2'000);
    isolatedList.AddHeatNode("recent-large", 1.0, now);
    HeatEvictionStrategy strategy(isolatedList, objectTable_, GetCurrentHeatPolicyConfig());

    EvictionRoundState round;
    EvictionCandidate candidate;
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    EXPECT_EQ(candidate.objectKey, "older-small");
}

TEST_F(HeatEvictionTest, SizeAwareSelectionPreservesExactRankingAboveFour)
{
    DS_ASSERT_OK(CreateObject("small-hot", 1 * KB, WriteMode::NONE_L2_CACHE_EVICT));
    DS_ASSERT_OK(CreateObject("large-hotter", 64 * KB, WriteMode::NONE_L2_CACHE_EVICT));

    const uint64_t now = GetSteadyClockTimeStampMs();
    EvictionList isolatedList;
    isolatedList.AddHeatNode("small-hot", 5.0, now - 3'000);
    isolatedList.AddHeatNode("large-hotter", 6.0, now - 4'000);
    HeatEvictionStrategy strategy(isolatedList, objectTable_, GetCurrentHeatPolicyConfig());

    EvictionRoundState round;
    EvictionCandidate candidate;
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    EXPECT_EQ(candidate.objectKey, "small-hot");
}

TEST_F(HeatEvictionTest, SizeAwareSelectionUsesDensityForIntermediateHeat)
{
    DS_ASSERT_OK(CreateObject("small-intermediate", 1 * KB, WriteMode::NONE_L2_CACHE_EVICT));
    DS_ASSERT_OK(CreateObject("large-intermediate", 64 * KB, WriteMode::NONE_L2_CACHE_EVICT));

    const uint64_t now = GetSteadyClockTimeStampMs();
    EvictionList isolatedList;
    isolatedList.AddHeatNode("small-intermediate", 3.0, now - 4'000);
    isolatedList.AddHeatNode("large-intermediate", 4.0, now - 3'000);
    HeatEvictionStrategy strategy(isolatedList, objectTable_, GetCurrentHeatPolicyConfig());

    EvictionRoundState round;
    EvictionCandidate candidate;
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    EXPECT_EQ(candidate.objectKey, "large-intermediate");
}

TEST_F(HeatEvictionTest, SizeAwareSelectionDefersCandidateWithoutStableShm)
{
    DS_ASSERT_OK(CreateObject("valid-large", 64 * KB, WriteMode::NONE_L2_CACHE_EVICT));

    const uint64_t now = GetSteadyClockTimeStampMs();
    EvictionList isolatedList;
    // An orphan eviction membership has no ObjectTable/ShmUnit size snapshot. It must not outrank a resolvable
    // candidate merely because it was encountered first; the existing final WLock path remains the cleanup fallback.
    isolatedList.AddHeatNode("valid-large", 1.0, now - 3'000);
    isolatedList.AddHeatNode("orphan", 0.5, now - 4'000);
    HeatEvictionStrategy strategy(isolatedList, objectTable_, GetCurrentHeatPolicyConfig());

    EvictionRoundState round;
    EvictionCandidate candidate;
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    EXPECT_EQ(candidate.objectKey, "valid-large");
}

TEST_F(HeatEvictionTest, StaleSelectedCandidateDoesNotDiscardRemainingSnapshotBatch)
{
    constexpr uint64_t payloadSize = 1 * KB;
    DS_ASSERT_OK(CreateObject("stale-first", payloadSize, WriteMode::NONE_L2_CACHE_EVICT));
    DS_ASSERT_OK(CreateObject("snapshot-survivor", payloadSize, WriteMode::NONE_L2_CACHE_EVICT));
    DS_ASSERT_OK(CreateObject("new-colder", payloadSize, WriteMode::NONE_L2_CACHE_EVICT));

    const uint64_t now = GetSteadyClockTimeStampMs();
    EvictionList isolatedList;
    isolatedList.AddHeatNode("stale-first", 0.5, now - 3'000);
    isolatedList.AddHeatNode("snapshot-survivor", 1.0, now - 2'000);
    HeatEvictionStrategy strategy(isolatedList, objectTable_, GetCurrentHeatPolicyConfig());

    EvictionRoundState round;
    EvictionCandidate candidate;
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    ASSERT_EQ(candidate.objectKey, "stale-first");
    strategy.OnCacheHit(candidate.objectKey, payloadSize);
    EXPECT_FALSE(strategy.ValidateCandidate(round, candidate));

    // New membership is intentionally deferred until the bounded snapshot batch is consumed. Invalidating one
    // selected node must not turn a 16-Block hit burst into a full scan/size-resolution/sort rebuild.
    isolatedList.AddHeatNode("new-colder", 0.1, now - 4'000);
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    EXPECT_EQ(candidate.objectKey, "snapshot-survivor");
}

TEST_F(HeatEvictionTest, StaleQueuedCandidateDoesNotDiscardRemainingSnapshotBatch)
{
    constexpr uint64_t payloadSize = 1 * KB;
    for (const auto &key : { "first", "stale-queued", "snapshot-survivor", "new-colder" }) {
        DS_ASSERT_OK(CreateObject(key, payloadSize, WriteMode::NONE_L2_CACHE_EVICT));
    }

    const uint64_t now = GetSteadyClockTimeStampMs();
    EvictionList isolatedList;
    isolatedList.AddHeatNode("first", 0.25, now - 4'000);
    isolatedList.AddHeatNode("stale-queued", 0.5, now - 3'000);
    isolatedList.AddHeatNode("snapshot-survivor", 1.0, now - 2'000);
    HeatEvictionStrategy strategy(isolatedList, objectTable_, GetCurrentHeatPolicyConfig());

    EvictionRoundState round;
    EvictionCandidate candidate;
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    ASSERT_EQ(candidate.objectKey, "first");
    ASSERT_TRUE(strategy.ValidateCandidate(round, candidate));
    DS_ASSERT_OK(isolatedList.Erase(candidate.objectKey));

    strategy.OnCacheHit("stale-queued", payloadSize);
    isolatedList.AddHeatNode("new-colder", 0.1, now - 5'000);
    DS_ASSERT_OK(strategy.SelectCandidate(round, candidate));
    EXPECT_EQ(candidate.objectKey, "snapshot-survivor");
}

TEST_F(HeatEvictionTest, SelectionDoesNotInventAnAccess)
{
    EvictionList clockList;
    clockList.Add("clock", 1);
    ClockEvictionStrategy clock(clockList, gRefTable_);
    EvictionRoundState clockRound;
    EvictionCandidate candidate;
    DS_ASSERT_OK(clock.SelectCandidate(clockRound, candidate));
    ASSERT_EQ(candidate.objectKey, "clock");
    EvictionList::Node clockNode;
    DS_ASSERT_OK(clockList.GetObjectInfo("clock", clockNode));
    EXPECT_EQ(clockNode.curCounter, 0);

    EvictionList heatList;
    heatList.AddHeatNode("heat", 1.0, 1000);
    HeatEvictionStrategy heat(heatList, objectTable_, GetCurrentHeatPolicyConfig());
    EvictionRoundState heatRound;
    DS_ASSERT_OK(heat.SelectCandidate(heatRound, candidate));
    ASSERT_EQ(candidate.objectKey, "heat");
    EvictionList::Node heatNode;
    DS_ASSERT_OK(heatList.GetObjectInfo("heat", heatNode));
    EXPECT_DOUBLE_EQ(heatNode.heat, 1.0);
}

TEST_F(HeatEvictionTest, SelectedHeatSurvivesEvictionRoundForAsyncRetry)
{
    EvictionList heatList;
    constexpr double selectedHeat = 7.5;
    heatList.AddHeatNode("heat", selectedHeat, 1000);
    HeatEvictionStrategy heat(heatList, objectTable_, GetCurrentHeatPolicyConfig());

    EvictionRoundState round;
    EvictionCandidate candidate;
    DS_ASSERT_OK(heat.SelectCandidate(round, candidate));
    ASSERT_EQ(candidate.objectKey, "heat");
    EvictionList::Node extracted;
    DS_ASSERT_OK(heatList.Extract(candidate.objectKey, extracted));
    heat.ReaddCandidate(candidate, 0);
    EvictionList::Node restored;
    DS_ASSERT_OK(heatList.GetObjectInfo(candidate.objectKey, restored));
    EXPECT_DOUBLE_EQ(restored.heat, selectedHeat);
}

// Periodic maintenance dispatches to the heat list and reduces heat over a half-life.
TEST_F(HeatEvictionTest, PeriodicMaintenanceReducesHeat)
{
    DS_ASSERT_OK(CreateObject("k1", 1024, WriteMode::NONE_L2_CACHE_EVICT, true));  // primary copy in object table
    // Seed heat=cap with a 120s-old lastDelay so the periodic decay has real elapsed time.
    const uint64_t now = GetSteadyClockTimeStampMs();
    SeedHeat("k1", 32.0, now - 120000);
    ASSERT_DOUBLE_EQ(HeatOf("k1"), 32.0);
    DS_ASSERT_OK(MaintainHeat());  // FLAGS primary half-life=120 -> factor~0.5 -> heat~16
    double after = HeatOf("k1");
    EXPECT_NEAR(after, 16.0, 4.0);
    EXPECT_LT(after, 32.0);
}

TEST_F(HeatEvictionTest, PeriodicMaintenanceDoesNotWaitForBusyObject)
{
    const uint64_t now = GetSteadyClockTimeStampMs();
    DS_ASSERT_OK(CreateObject("busy-primary", 1024, WriteMode::NONE_L2_CACHE_EVICT, true));
    SeedHeat("busy-primary", 32.0, now - 120'000);
    std::shared_ptr<SafeObjType> busyEntry;
    DS_ASSERT_OK(objectTable_->Get("busy-primary", busyEntry));
    DS_ASSERT_OK(busyEntry->WLock(true));
    Raii unlock([&busyEntry]() { busyEntry->WUnlock(); });

    auto decay = std::async(std::launch::async, [this]() { return MaintainHeat(); });
    EXPECT_EQ(decay.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    DS_ASSERT_OK(decay.get());
    EXPECT_DOUBLE_EQ(HeatOf("busy-primary"), 32.0);
}

// Add (first publish/create) seeds heat at the low initial value (not the cap), so fresh inserts are not
// counted as hot data by the heat rebalance strategy (heat > rebalance_heat_hot_counter_threshold). A fresh
// object stays at the eviction threshold so it is not a first-round eviction candidate either.
TEST_F(HeatEvictionTest, AddSeedsInitialHeatNotCap)
{
    evictionManager_->Add("k1");
    EXPECT_DOUBLE_EQ(HeatOf("k1"), 2.0);  // FLAGS_eviction_heat_initial_counter, not the cap (32)
}

TEST_F(HeatEvictionTest, PolicyPrecheckIsReadOnlyAndEnforcesAdmissionLimits)
{
    const auto now = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    SeedHeat("first", 2.0, now);
    SeedHeat("second", 4.0, now);
    uint64_t sourceObjects = 0;
    EXPECT_EQ(evictionManager_->PrecheckPolicyUpdate(EvictionPolicy::CLOCK, 1, 16, 0, 1, 0, sourceObjects).GetCode(),
              K_NO_SPACE);
    EXPECT_EQ(sourceObjects, 2);
    EXPECT_EQ(evictionManager_
                  ->PrecheckPolicyUpdate(EvictionPolicy::CLOCK, 1, 16, std::numeric_limits<uint64_t>::max(), 0, 0,
                                         sourceObjects)
                  .GetCode(),
              K_NO_SPACE);
    EXPECT_EQ(evictionManager_->PrecheckPolicyUpdate(EvictionPolicy::CLOCK, 1, 16, 0, 0, 1, sourceObjects).GetCode(),
              K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(evictionManager_
                  ->PrecheckPolicyUpdate(EvictionPolicy::CLOCK, 1,
                                         EVICTION_POLICY_MAX_MIGRATION_BATCH_SIZE + 1, 0, 0, 0, sourceObjects)
                  .GetCode(),
              K_INVALID);
    DS_ASSERT_OK(evictionManager_->PrecheckPolicyUpdate(EvictionPolicy::CLOCK, 1, 16, 0, 2, 0, sourceObjects));
    EXPECT_EQ(sourceObjects, 2);
    EXPECT_EQ(PolicyPhase(), WorkerOcEvictionManager::PolicyUpdatePhase::STABLE);
    EXPECT_EQ(evictionManager_->GetActiveEvictionPolicy(), EvictionPolicy::HEAT);
    uint64_t totalObjects = 0;
    uint64_t migratedObjects = 0;
    evictionManager_->GetPolicyUpdateProgress(totalObjects, migratedObjects);
    EXPECT_EQ(totalObjects, 2);
    EXPECT_EQ(migratedObjects, 0);
}

TEST_F(HeatEvictionTest, HotUpdateConvertsInBoundedBatchesAndPublishesAtomically)
{
    const auto now = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    SeedHeat("cold", 1.0, now);
    SeedHeat("warm", 3.0, now);
    SeedHeat("hot", 8.0, now);

    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 1));
    EXPECT_EQ(PolicyPhase(), WorkerOcEvictionManager::PolicyUpdatePhase::MIGRATING);
    evictionManager_->Evict();
    EXPECT_FALSE(evictionManager_->IsRunning());

    // target-first: the foreground hit creates the Clock target membership.
    // The later source migration merges into that membership instead of creating
    // a second target entry.
    evictionManager_->OnCacheHit("warm");
    DS_ASSERT_OK(CreateObject("migrated", 1, WriteMode::NONE_L2_CACHE_EVICT));
    DS_ASSERT_OK(evictionManager_->ApplyMigratedHeat("migrated", 8.0, true));
    bool done = false;
    while (!done) {
        DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(1, done));
    }
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(1));

    EXPECT_EQ(evictionManager_->GetActiveEvictionPolicy(), EvictionPolicy::CLOCK);
    EXPECT_EQ(NodeOf("cold").curCounter, 0);
    EXPECT_EQ(NodeOf("warm").curCounter, 1);
    EXPECT_EQ(NodeOf("hot").curCounter, 2);
    EXPECT_EQ(NodeOf("migrated").curCounter, 2);
}

TEST_F(HeatEvictionTest, PolicySnapshotConversionExceptionRestoresSourceMembership)
{
    constexpr const char *injectPoint = "WorkerOcEvictionManager.MoveOnePolicyNode.beforeConvert";
    SeedHeat("key", 8.0, static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 2));
    DS_ASSERT_OK(inject::Set(injectPoint, "1*call()"));
    Raii clearInjectPoint([injectPoint]() { (void)inject::Clear(injectPoint); });

    bool done = false;
    EXPECT_EQ(evictionManager_->MigratePolicyBatch(1, done).GetCode(), K_RUNTIME_ERROR);
    EXPECT_FALSE(done);
    EXPECT_EQ(NodeOf("key").objectKey, "key");

    DS_ASSERT_OK(inject::Clear(injectPoint));
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(1, done));
    EXPECT_TRUE(done);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(2));
    EXPECT_EQ(evictionManager_->GetActiveEvictionPolicy(), EvictionPolicy::CLOCK);
}

TEST_F(HeatEvictionTest, PolicyUpdateWaitsForPrimaryEndLifeReaddToDrain)
{
    constexpr uint64_t version = 7;
    SeedHeat("pending", 4.0, static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
    MarkPrimaryEndLifeTaskActive("pending", version);

    auto begin = std::async(std::launch::async,
                            [this]() { return evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 1); });
    constexpr auto phaseTimeout = std::chrono::seconds(2);
    const auto phaseDeadline = std::chrono::steady_clock::now() + phaseTimeout;
    while (PolicyPhase() != WorkerOcEvictionManager::PolicyUpdatePhase::DRAINING
           && std::chrono::steady_clock::now() < phaseDeadline) {
        std::this_thread::yield();
    }
    if (PolicyPhase() != WorkerOcEvictionManager::PolicyUpdatePhase::DRAINING) {
        FinishPrimaryEndLifeTaskAndWorker("pending", version);
        begin.wait();
        FAIL() << "Policy update did not enter draining while the primary end-life task was held";
    }
    EXPECT_EQ(begin.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    FinishPrimaryEndLifeTaskAndWorker("pending", version);
    ASSERT_EQ(begin.wait_for(phaseTimeout), std::future_status::ready);
    DS_ASSERT_OK(begin.get());
    EXPECT_EQ(PolicyPhase(), WorkerOcEvictionManager::PolicyUpdatePhase::MIGRATING);
}

TEST_F(HeatEvictionTest, PolicyUpdateWaitsForShardedStableRouteReader)
{
    std::promise<void> enteredPromise;
    auto entered = enteredPromise.get_future();
    std::promise<void> releasePromise;
    auto release = releasePromise.get_future().share();
    auto reader = std::async(std::launch::async, [this, &enteredPromise, release]() {
        HoldStableRouteReader([&enteredPromise, release]() {
            enteredPromise.set_value();
            release.wait();
        });
    });
    if (entered.wait_for(std::chrono::seconds(2)) != std::future_status::ready) {
        releasePromise.set_value();
        reader.wait();
        FAIL() << "Stable route reader did not enter";
    }

    auto begin = std::async(std::launch::async,
                            [this]() { return evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 1); });
    const auto phaseDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (PolicyPhase() != WorkerOcEvictionManager::PolicyUpdatePhase::DRAINING
           && std::chrono::steady_clock::now() < phaseDeadline) {
        std::this_thread::yield();
    }
    if (PolicyPhase() != WorkerOcEvictionManager::PolicyUpdatePhase::DRAINING) {
        releasePromise.set_value();
        reader.wait();
        begin.wait();
        FAIL() << "Policy update did not enter draining while the stable reader was held";
    }
    EXPECT_EQ(begin.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    releasePromise.set_value();
    ASSERT_EQ(reader.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    reader.get();
    ASSERT_EQ(begin.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    DS_ASSERT_OK(begin.get());
    EXPECT_EQ(PolicyPhase(), WorkerOcEvictionManager::PolicyUpdatePhase::MIGRATING);
}

TEST_F(HeatEvictionTest, ConcurrentHitAndBackgroundMigrationMergeWithoutLosingTargetHeat)
{
    const auto now = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    SeedHeat("key", 8.0, now);
    bool done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 20));
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(20));

    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::HEAT, 21));
    std::thread hits([this]() {
        for (int i = 0; i < 100; ++i) {
            evictionManager_->OnCacheHit("key");
        }
    });
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    hits.join();
    ASSERT_TRUE(done);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(21));
    EXPECT_DOUBLE_EQ(NodeOf("key").heat, 32.0);
}

TEST_F(HeatEvictionTest, HotUpdateSupportsRepeatedClockHeatSwitches)
{
    const auto now = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    SeedHeat("key", 8.0, now);
    bool done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 10));
    EXPECT_TRUE(evictionManager_->NeedsMigratableSize());
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(10));
    EXPECT_FALSE(evictionManager_->NeedsMigratableSize());

    done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::HEAT, 11));
    EXPECT_TRUE(evictionManager_->NeedsMigratableSize());
    // Migrated heat may arrive before the background scan reaches this key. It must
    // create target-first membership and later merge the frozen source snapshot.
    DS_ASSERT_OK(evictionManager_->ApplyMigratedHeat("key", 10.0, true));
    EXPECT_DOUBLE_EQ(NodeOf("key").heat, 10.0);
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(11));
    EXPECT_TRUE(evictionManager_->NeedsMigratableSize());
    EXPECT_EQ(evictionManager_->GetActiveEvictionPolicy(), EvictionPolicy::HEAT);
    EXPECT_DOUBLE_EQ(NodeOf("key").heat, 14.0);
}

TEST_F(HeatEvictionTest, ClockFastPathCannotApplyUnknownSizeAfterHeatTransitionStarts)
{
    constexpr const char *injectPoint = "WorkerOcEvictionManager.TryClockMutation.afterSizeCheck";
    constexpr uint64_t objectSize = 1024 * 1024;
    SeedHeat("key", 8.0, static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
    bool done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 12));
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(12));

    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInjectPoint([injectPoint]() { (void)inject::Clear(injectPoint); });
    auto hit = std::async(std::launch::async, [this]() {
        if (!evictionManager_->TryOnCacheHitWithoutSize("key")) {
            evictionManager_->OnCacheHit("key", objectSize);
        }
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (inject::GetExecuteCount(injectPoint) == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const bool reachedInjectPoint = inject::GetExecuteCount(injectPoint) > 0;
    if (!reachedInjectPoint) {
        DS_ASSERT_OK(inject::Clear(injectPoint));
        ASSERT_EQ(hit.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        hit.get();
        FAIL() << "CLOCK fast-path thread did not reach the transition injection point";
    }

    Status beginRc = evictionManager_->BeginPolicyUpdate(EvictionPolicy::HEAT, 13);
    DS_ASSERT_OK(inject::Clear(injectPoint));
    ASSERT_EQ(hit.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    hit.get();
    DS_ASSERT_OK(beginRc);

    constexpr double expectedCredit = 4096.0 / objectSize;
    EXPECT_NEAR(NodeOf("key").heat, 4.0 + expectedCredit, 1e-9);
}

TEST_F(HeatEvictionTest, PolicyStateSnapshotReportsOneCoherentTransitionState)
{
    auto stable = evictionManager_->GetPolicyStateSnapshot();
    EXPECT_EQ(stable.phase, WorkerOcEvictionManager::PolicyUpdatePhase::STABLE);
    EXPECT_EQ(stable.activePolicy, EvictionPolicy::HEAT);
    EXPECT_EQ(stable.epoch, 0u);

    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 39));
    auto migrating = evictionManager_->GetPolicyStateSnapshot();
    EXPECT_EQ(migrating.phase, WorkerOcEvictionManager::PolicyUpdatePhase::MIGRATING);
    EXPECT_EQ(migrating.activePolicy, EvictionPolicy::HEAT);
    EXPECT_EQ(migrating.targetPolicy, EvictionPolicy::CLOCK);
    EXPECT_EQ(migrating.epoch, 39u);

    bool done = false;
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(39));
}

TEST_F(HeatEvictionTest, CommitRejectsTargetMembershipWithoutResidentObject)
{
    SeedOrphanHeat("orphan", 3.0, GetSteadyClockTimeStampMs());
    bool done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 40));
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);
    EXPECT_TRUE(evictionManager_->CommitPolicyUpdate(40).IsError());
    EXPECT_EQ(PolicyPhase(), WorkerOcEvictionManager::PolicyUpdatePhase::VERIFYING);
}

TEST_F(HeatEvictionTest, CommitRejectsResidentObjectWithoutTargetMembership)
{
    DS_ASSERT_OK(CreateObject("missing", 1, WriteMode::NONE_L2_CACHE_EVICT));
    bool done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 41));
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);
    EXPECT_TRUE(evictionManager_->CommitPolicyUpdate(41).IsError());
    EXPECT_EQ(PolicyPhase(), WorkerOcEvictionManager::PolicyUpdatePhase::VERIFYING);
}

TEST_F(HeatEvictionTest, CommitRetriesWhenMembershipChangesAfterAudit)
{
    constexpr const char *injectPoint = "WorkerOcEvictionManager.CommitPolicyUpdate.afterAudit";
    constexpr auto waitTimeout = std::chrono::seconds(2);
    SeedHeat("key", 3.0, static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
    bool done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 42));
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);

    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInjectPoint([injectPoint]() { (void)inject::Clear(injectPoint); });
    Status commitStatus;
    std::thread commit([this, &commitStatus]() { commitStatus = evictionManager_->CommitPolicyUpdate(42); });
    const auto deadline = std::chrono::steady_clock::now() + waitTimeout;
    while (inject::GetExecuteCount(injectPoint) == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const bool auditCompleted = inject::GetExecuteCount(injectPoint) > 0;
    if (auditCompleted) {
        evictionManager_->OnCacheHit("key");
    }
    DS_ASSERT_OK(inject::Clear(injectPoint));
    commit.join();

    ASSERT_TRUE(auditCompleted);
    EXPECT_EQ(commitStatus.GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(PolicyPhase(), WorkerOcEvictionManager::PolicyUpdatePhase::VERIFYING);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(42));
    EXPECT_EQ(evictionManager_->GetActiveEvictionPolicy(), EvictionPolicy::CLOCK);
}

TEST_F(HeatEvictionTest, MembershipAuditDoesNotHoldPolicyRouteLockDuringObjectScan)
{
    constexpr const char *injectPoint = "WorkerOcEvictionManager.AuditPolicyUpdateMembership.afterRouteSnapshot";
    constexpr auto waitTimeout = std::chrono::seconds(2);
    SeedHeat("key", 3.0, static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
    bool done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 43));
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);

    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    auto commit = std::async(std::launch::async, [this]() { return evictionManager_->CommitPolicyUpdate(43); });
    Raii clearInjectPoint([injectPoint]() { (void)inject::Clear(injectPoint); });
    const auto deadline = std::chrono::steady_clock::now() + waitTimeout;
    while (inject::GetExecuteCount(injectPoint) == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    if (inject::GetExecuteCount(injectPoint) == 0) {
        (void)inject::Clear(injectPoint);
        commit.wait();
        FAIL() << "Membership audit did not reach the route snapshot boundary";
    }

    auto snapshot = std::async(std::launch::async, [this]() { return evictionManager_->GetPolicyStateSnapshot(); });
    const auto snapshotStatus = snapshot.wait_for(std::chrono::milliseconds(500));
    DS_ASSERT_OK(inject::Clear(injectPoint));
    ASSERT_EQ(snapshot.wait_for(waitTimeout), std::future_status::ready);
    EXPECT_EQ(snapshot.get().phase, WorkerOcEvictionManager::PolicyUpdatePhase::VERIFYING);
    EXPECT_EQ(snapshotStatus, std::future_status::ready);
    ASSERT_EQ(commit.wait_for(waitTimeout), std::future_status::ready);
    DS_ASSERT_OK(commit.get());
}

TEST_F(HeatEvictionTest, MembershipAuditReleasesObjectTableLockAfterSnapshot)
{
    constexpr const char *injectPoint =
        "WorkerOcEvictionManager.AuditPolicyUpdateMembership.afterObjectSnapshot";
    constexpr auto waitTimeout = std::chrono::seconds(2);
    SeedHeat("key", 3.0, static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
    bool done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 44));
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);

    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    auto commit = std::async(std::launch::async, [this]() { return evictionManager_->CommitPolicyUpdate(44); });
    Raii clearInjectPoint([injectPoint]() { (void)inject::Clear(injectPoint); });
    const auto deadline = std::chrono::steady_clock::now() + waitTimeout;
    while (inject::GetExecuteCount(injectPoint) == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    if (inject::GetExecuteCount(injectPoint) == 0) {
        (void)inject::Clear(injectPoint);
        commit.wait();
        FAIL() << "Membership audit did not finish taking the object-table snapshot";
    }

    auto insert = std::async(std::launch::async, [this]() {
        RETURN_IF_NOT_OK(CreateObject("late", 1, WriteMode::NONE_L2_CACHE_EVICT));
        evictionManager_->Add("late");
        return Status::OK();
    });
    const auto insertStatus = insert.wait_for(std::chrono::milliseconds(500));
    DS_ASSERT_OK(inject::Clear(injectPoint));
    ASSERT_EQ(insert.wait_for(waitTimeout), std::future_status::ready);
    DS_ASSERT_OK(insert.get());
    EXPECT_EQ(insertStatus, std::future_status::ready);
    ASSERT_EQ(commit.wait_for(waitTimeout), std::future_status::ready);
    EXPECT_EQ(commit.get().GetCode(), K_TRY_AGAIN);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(44));
}

TEST_F(HeatEvictionTest, WorkerPolicyStateRecoversLastGoodAndUnfinishedIntent)
{
    using State = WorkerOcEvictionManager::PersistedPolicyState;
    auto persisted = std::make_shared<std::optional<State>>();
    auto loader = [persisted](State &state, bool &found) {
        found = persisted->has_value();
        if (found) {
            state = **persisted;
        }
        return Status::OK();
    };
    auto storer = [persisted](const State &state) {
        *persisted = state;
        return Status::OK();
    };
    DS_ASSERT_OK(evictionManager_->InitPolicyStateStore(loader, storer));
    SeedHeat("key", 4.0, GetSteadyClockTimeStampMs());
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 50));
    ASSERT_TRUE(persisted->has_value());
    EXPECT_TRUE((*persisted)->hasTransitionIntent);
    EXPECT_EQ((*persisted)->activePolicy, EvictionPolicy::HEAT);
    EXPECT_EQ((*persisted)->targetPolicy, EvictionPolicy::CLOCK);
    EXPECT_EQ((*persisted)->transitionEpoch, 50u);

    auto recovered = std::make_shared<WorkerOcEvictionManager>(std::make_shared<ObjectTable>(), workerAddr_,
                                                               workerAddr_, GetTestMetadataRoute());
    DS_ASSERT_OK(recovered->InitPolicyStateStore(loader, storer));
    EXPECT_EQ(recovered->GetActiveEvictionPolicy(), EvictionPolicy::HEAT);
    EXPECT_EQ(recovered->GetPolicyUpdateEpoch(), 0u);

    uint64_t sourceObjects = 0;
    DS_ASSERT_OK(recovered->PrecheckPolicyUpdate(EvictionPolicy::CLOCK, 50, 8, 0, 0, 0, sourceObjects));
    bool complete = false;
    DS_ASSERT_OK(recovered->HandlePolicyUpdate(EvictionPolicy::CLOCK, 50, 8, complete));
    EXPECT_TRUE(complete);
    EXPECT_EQ(recovered->GetActiveEvictionPolicy(), EvictionPolicy::CLOCK);
    ASSERT_TRUE(persisted->has_value());
    EXPECT_FALSE((*persisted)->hasTransitionIntent);
    EXPECT_EQ((*persisted)->activeEpoch, 50u);

    auto lastGood = std::make_shared<WorkerOcEvictionManager>(std::make_shared<ObjectTable>(), workerAddr_, workerAddr_,
                                                              GetTestMetadataRoute());
    DS_ASSERT_OK(lastGood->InitPolicyStateStore(loader, storer));
    EXPECT_EQ(lastGood->GetActiveEvictionPolicy(), EvictionPolicy::CLOCK);
    EXPECT_EQ(lastGood->GetPolicyUpdateEpoch(), 50u);
}

TEST_F(HeatEvictionTest, PolicyUpdateDoesNotDrainWhenIntentPersistenceFails)
{
    using State = WorkerOcEvictionManager::PersistedPolicyState;
    auto rejectWrites = std::make_shared<std::atomic<bool>>(false);
    auto loader = [](State &, bool &found) {
        found = false;
        return Status::OK();
    };
    auto storer = [rejectWrites](const State &) {
        return rejectWrites->load() ? Status(K_IO_ERROR, "injected persistence failure") : Status::OK();
    };
    DS_ASSERT_OK(evictionManager_->InitPolicyStateStore(loader, storer));
    rejectWrites->store(true);
    EXPECT_TRUE(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 60).IsError());
    EXPECT_EQ(PolicyPhase(), WorkerOcEvictionManager::PolicyUpdatePhase::STABLE);
    EXPECT_EQ(evictionManager_->GetActiveEvictionPolicy(), EvictionPolicy::HEAT);
}

TEST_F(HeatEvictionTest, CopyWatermarkUsesAllocatorAccountingAndStablePrimaryFilter)
{
    constexpr uint64_t payloadSize = 1;
    const uint64_t nowMs = GetSteadyClockTimeStampMs();
    DS_ASSERT_OK(CreateObject("hot-primary", payloadSize, WriteMode::NONE_L2_CACHE_EVICT, true));
    DS_ASSERT_OK(CreateObject("warm-primary", payloadSize, WriteMode::NONE_L2_CACHE_EVICT, true));
    DS_ASSERT_OK(CreateObject("hot-local", payloadSize, WriteMode::NONE_L2_CACHE_EVICT, false));
    DS_ASSERT_OK(CreateObject("invalid-primary", payloadSize, WriteMode::NONE_L2_CACHE_EVICT, true));
    DS_ASSERT_OK(CreateObject("incomplete-primary", payloadSize, WriteMode::NONE_L2_CACHE_EVICT, true));
    DS_ASSERT_OK(CreateObject("deleting-primary", payloadSize, WriteMode::NONE_L2_CACHE_EVICT, true));
    auto noShm = std::make_unique<ObjCacheShmUnit>();
    noShm->SetDataSize(payloadSize);
    noShm->SetLifeState(ObjectLifeState::OBJECT_SEALED);
    noShm->stateInfo.SetPrimaryCopy(true);
    DS_ASSERT_OK(objectTable_->Insert("no-shm-primary", std::move(noShm)));
    SeedHeat("hot-primary", 5.0, nowMs);
    SeedHeat("warm-primary", 3.0, nowMs);
    SeedHeat("hot-local", 8.0, nowMs);
    SeedHeat("invalid-primary", 8.0, nowMs);
    SeedHeat("incomplete-primary", 8.0, nowMs);
    SeedHeat("deleting-primary", 8.0, nowMs);
    SeedHeat("no-shm-primary", 8.0, nowMs);

    auto getEntry = [this](const std::string &key) {
        std::shared_ptr<SafeObjType> entry;
        EXPECT_TRUE(objectTable_->Get(key, entry).IsOk());
        return entry;
    };
    auto invalid = getEntry("invalid-primary");
    auto incomplete = getEntry("incomplete-primary");
    auto deleting = getEntry("deleting-primary");
    DS_ASSERT_OK(invalid->WLock(true));
    invalid->Get()->stateInfo.SetCacheInvalid(true);
    invalid->WUnlock();
    DS_ASSERT_OK(incomplete->WLock(true));
    incomplete->Get()->stateInfo.SetIncompleted(true);
    incomplete->WUnlock();
    DS_ASSERT_OK(deleting->WLock(true));
    deleting->Get()->stateInfo.SetNeedToDelete(true);
    deleting->WUnlock();

    auto migratableSize = [&getEntry](const std::string &key) {
        auto entry = getEntry(key);
        const auto rc = entry->RLock(true);
        EXPECT_TRUE(rc.IsOk());
        if (rc.IsError()) {
            return uint64_t{ 0 };
        }
        Raii unlock([&entry]() { entry->RUnlock(); });
        auto shmUnit = entry->Get()->GetShmUnit();
        EXPECT_NE(shmUnit, nullptr);
        return shmUnit == nullptr ? uint64_t{ 0 } : shmUnit->GetMigratableSize();
    };
    const uint64_t hotPrimaryBytes = migratableSize("hot-primary");
    const uint64_t warmPrimaryBytes = migratableSize("warm-primary");

    WorkerOcEvictionManager::CopyWatermarkStats stats;
    DS_ASSERT_OK(evictionManager_->MaintainHeatAndCollectHotPrimaryStats(stats));
    EXPECT_EQ(stats.coldPrimaryCopyCount, 0u);
    EXPECT_EQ(stats.warmPrimaryCopyCount, 1u);
    EXPECT_EQ(stats.hotPrimaryCopyCount, 1u);
    EXPECT_EQ(stats.totalPrimaryCopyCount, 2u);
    EXPECT_EQ(stats.coldPrimaryCopyBytes, 0u);
    EXPECT_EQ(stats.warmPrimaryCopyBytes, warmPrimaryBytes);
    EXPECT_EQ(stats.hotPrimaryCopyBytes, hotPrimaryBytes);
    EXPECT_EQ(stats.totalPrimaryCopyBytes, hotPrimaryBytes + warmPrimaryBytes);
    EXPECT_GT(stats.hotPrimaryCopyBytes, payloadSize);
    EXPECT_LE(stats.hotPrimaryCopyBytes, stats.totalPrimaryCopyBytes);

    uint64_t primaryCount = 0;
    uint64_t primaryBytes = 0;
    DS_ASSERT_OK(evictionManager_->CollectPrimaryCopyStats(primaryCount, primaryBytes));
    EXPECT_EQ(primaryCount, 2u);
    EXPECT_EQ(primaryBytes, stats.totalPrimaryCopyBytes);
}

TEST_F(HeatEvictionTest, PreReportCopyWatermarkIsReadOnlyAndUsesPolicySpecificHotDefinition)
{
    constexpr uint64_t payloadSize = 1;
    const uint64_t oldTimestamp = GetSteadyClockTimeStampMs() - 60'000;
    DS_ASSERT_OK(CreateObject("cold-primary", payloadSize, WriteMode::NONE_L2_CACHE_EVICT, true));
    DS_ASSERT_OK(CreateObject("warm-primary", payloadSize, WriteMode::NONE_L2_CACHE_EVICT, true));
    DS_ASSERT_OK(CreateObject("hot-primary", payloadSize, WriteMode::NONE_L2_CACHE_EVICT, true));
    SeedHeat("cold-primary", 1.0, oldTimestamp);
    SeedHeat("warm-primary", 2.0, oldTimestamp);
    SeedHeat("hot-primary", 32.0, oldTimestamp);

    WorkerOcEvictionManager::CopyWatermarkStats observed;
    evictionManager_->SetCopyWatermarkObserver(
        [&observed](const WorkerOcEvictionManager::CopyWatermarkStats &stats) { observed = stats; });
    RefreshCopyWatermarkSnapshot();
    EXPECT_EQ(observed.policy, EvictionPolicy::HEAT);
    EXPECT_EQ(observed.coldPrimaryCopyCount, 1u);
    EXPECT_EQ(observed.warmPrimaryCopyCount, 1u);
    EXPECT_EQ(observed.hotPrimaryCopyCount, 1u);
    EXPECT_EQ(observed.totalPrimaryCopyCount, 3u);
    EXPECT_EQ(observed.coldPrimaryCopyBytes + observed.warmPrimaryCopyBytes + observed.hotPrimaryCopyBytes,
              observed.totalPrimaryCopyBytes);
    EXPECT_DOUBLE_EQ(observed.counterP50, 2.0);
    EXPECT_DOUBLE_EQ(observed.counterP90, 32.0);
    EXPECT_EQ(observed.cappedPrimaryCopyCount, 1u);
    // Event telemetry must not perform the periodic decay that is owned by the 30-second maintenance hook.
    EXPECT_DOUBLE_EQ(HeatOf("cold-primary"), 1.0);
    EXPECT_DOUBLE_EQ(HeatOf("warm-primary"), 2.0);
    EXPECT_DOUBLE_EQ(HeatOf("hot-primary"), 32.0);

    WorkerOcEvictionManager::CopyWatermarkStats reportStats;
    evictionManager_->SetHotPrimaryReportObserver(
        [&reportStats](const WorkerOcEvictionManager::CopyWatermarkStats &stats) { reportStats = stats; });
    evictionManager_->RefreshHotPrimaryReport();
    EXPECT_EQ(reportStats.hotPrimaryCopyCount, 1u);
    EXPECT_EQ(reportStats.totalPrimaryCopyCount, 3u);
    EXPECT_DOUBLE_EQ(HeatOf("cold-primary"), 1.0);
    EXPECT_DOUBLE_EQ(HeatOf("warm-primary"), 2.0);
    EXPECT_DOUBLE_EQ(HeatOf("hot-primary"), 32.0);

    bool done = false;
    DS_ASSERT_OK(evictionManager_->BeginPolicyUpdate(EvictionPolicy::CLOCK, 100));
    DS_ASSERT_OK(evictionManager_->MigratePolicyBatch(8, done));
    ASSERT_TRUE(done);
    DS_ASSERT_OK(evictionManager_->CommitPolicyUpdate(100));

    const auto clockStats = CopyWatermarkStats();
    EXPECT_EQ(clockStats.policy, EvictionPolicy::CLOCK);
    EXPECT_EQ(clockStats.coldPrimaryCopyCount, 1u);
    EXPECT_EQ(clockStats.warmPrimaryCopyCount, 1u);
    EXPECT_EQ(clockStats.hotPrimaryCopyCount, 1u);  // Clock counter >= Q2 is hot.
    EXPECT_EQ(clockStats.totalPrimaryCopyCount, 3u);
    EXPECT_EQ(clockStats.coldPrimaryCopyBytes + clockStats.warmPrimaryCopyBytes + clockStats.hotPrimaryCopyBytes,
              clockStats.totalPrimaryCopyBytes);
    EXPECT_DOUBLE_EQ(clockStats.counterP50, 1.0);
    EXPECT_DOUBLE_EQ(clockStats.counterP90, 2.0);
    EXPECT_EQ(clockStats.cappedPrimaryCopyCount, 0u);

}

TEST_F(HeatEvictionTest, CopyWatermarkObserverSnapshotSurvivesConcurrentReplacement)
{
    auto lifetime = std::make_shared<int>(1);
    std::weak_ptr<int> weakLifetime = lifetime;
    std::promise<void> enteredPromise;
    auto entered = enteredPromise.get_future();
    std::promise<void> releasePromise;
    auto release = releasePromise.get_future().share();
    evictionManager_->SetCopyWatermarkObserver(
        [lifetime, &enteredPromise, release](const WorkerOcEvictionManager::CopyWatermarkStats &) {
            enteredPromise.set_value();
            release.wait();
        });

    std::thread notifier([this]() { NotifyCopyWatermarkObserver(); });
    if (entered.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        releasePromise.set_value();
        notifier.join();
        FAIL() << "Copy-watermark observer was not invoked";
    }
    evictionManager_->SetCopyWatermarkObserver(nullptr);
    lifetime.reset();
    EXPECT_FALSE(weakLifetime.expired());
    releasePromise.set_value();
    notifier.join();
    EXPECT_TRUE(weakLifetime.expired());

    std::atomic<uint64_t> notifications{ 0 };
    std::thread setter([&]() {
        for (uint64_t i = 0; i < 1'000; ++i) {
            if ((i & 1U) == 0) {
                evictionManager_->SetCopyWatermarkObserver(
                    [&notifications](const WorkerOcEvictionManager::CopyWatermarkStats &) {
                        notifications.fetch_add(1, std::memory_order_relaxed);
                    });
            } else {
                evictionManager_->SetCopyWatermarkObserver(nullptr);
            }
        }
    });
    std::thread concurrentNotifier([this]() {
        for (uint64_t i = 0; i < 1'000; ++i) {
            NotifyCopyWatermarkObserver();
        }
    });
    setter.join();
    concurrentNotifier.join();
    evictionManager_->SetCopyWatermarkObserver(nullptr);
}

}  // namespace ut
}  // namespace datasystem
