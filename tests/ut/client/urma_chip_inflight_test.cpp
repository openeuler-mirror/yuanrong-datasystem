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

#include <gtest/gtest.h>

#ifdef USE_URMA
#define private public
#include "datasystem/common/rdma/urma_manager.h"
#undef private
#include "datasystem/common/util/raii.h"

namespace datasystem {
namespace {

TEST(UrmaChipInflightTest, FormatsNonZeroCountsWithRealChipIds)
{
    auto &manager = UrmaManager::Instance();
    EXPECT_EQ(manager.GetSrcChipInflightWrCounter(1), &manager.srcChipInflightWrCounts_.at(1).value);
    EXPECT_EQ(manager.GetSrcChipInflightWrCounter(2), &manager.srcChipInflightWrCounts_.at(2).value);
    EXPECT_EQ(manager.GetSrcChipInflightWrCounter(INVALID_CHIP_ID), nullptr);
    manager.srcChipInflightWrCounts_.at(1).value.store(3, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(5, std::memory_order_relaxed);

    EXPECT_STREQ(manager.GetSrcChipInflightWrCountsString(), "{1:3,2:5}");

    manager.srcChipInflightWrCounts_.at(1).value.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
}

TEST(UrmaChipInflightTest, RecordsCumulativeNumaWriteCountsBySourceAndDestinationChip)
{
    auto &manager = UrmaManager::Instance();
    const auto resetCounts = [&manager] {
        manager.srcChipWriteCounts_.at(1).store(0, std::memory_order_relaxed);
        manager.srcChipWriteCounts_.at(2).store(0, std::memory_order_relaxed);
        manager.dstChipWriteCounts_.at(1).store(0, std::memory_order_relaxed);
        manager.dstChipWriteCounts_.at(2).store(0, std::memory_order_relaxed);
        manager.src1Src2WriteCount_.store(0, std::memory_order_relaxed);
        manager.src2Src1WriteCount_.store(0, std::memory_order_relaxed);
    };
    resetCounts();
    Raii cleanup(resetCounts);

    manager.RecordNumaWriteChipCounts(1, 2);
    manager.RecordNumaWriteChipCounts(2, 1);
    manager.RecordNumaWriteChipCounts(INVALID_CHIP_ID, INVALID_CHIP_ID);

    EXPECT_STREQ(manager.GetNumaWriteChipCountsString(),
                 "{src1:1,src2:1,dst1:1,dst2:1,src1_src2:0,src2_src1:0}");

    manager.RecordNumaWriteSourceSwitchCount(1, 2);
    manager.RecordNumaWriteSourceSwitchCount(2, 1);
    manager.RecordNumaWriteSourceSwitchCount(1, 1);
    manager.RecordNumaWriteSourceSwitchCount(2, 2);
    manager.RecordNumaWriteSourceSwitchCount(INVALID_CHIP_ID, 2);
    manager.RecordNumaWriteSourceSwitchCount(1, INVALID_CHIP_ID);
    EXPECT_STREQ(manager.GetNumaWriteChipCountsString(),
                 "{src1:1,src2:1,dst1:1,dst2:1,src1_src2:1,src2_src1:1}");
}

TEST(UrmaChipInflightTest, TracksBothSourceChipCountersForEventLifetime)
{
    std::atomic<int> chip1Counter{ 0 };
    std::atomic<int> chip2Counter{ 0 };
    auto chip1Event = std::make_shared<UrmaEvent>(1, nullptr, "remote", "instance", 1,
                                                  UrmaEvent::OperationType::WRITE, &chip1Counter);
    auto chip2Event = std::make_shared<UrmaEvent>(2, nullptr, "remote", "instance", 1,
                                                  UrmaEvent::OperationType::WRITE, &chip2Counter);

    EXPECT_EQ(chip1Counter.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(chip2Counter.load(std::memory_order_relaxed), 1);
    chip1Event.reset();
    chip2Event.reset();
    EXPECT_EQ(chip1Counter.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(chip2Counter.load(std::memory_order_relaxed), 0);
}

TEST(UrmaChipInflightTest, SkipsClientTransportNumaBindingWhenTopologyIsEmpty)
{
    auto &manager = UrmaManager::Instance();
    EXPECT_TRUE(manager.BindClientTransportMemoryToNumaNodes(nullptr, 8192, {}).IsOk());
}

TEST(UrmaChipInflightTest, RejectsInvalidClientTransportNumaBindingWithKnownTopology)
{
    auto &manager = UrmaManager::Instance();
    EXPECT_EQ(manager.BindClientTransportMemoryToNumaNodes(nullptr, 8192, { 0 }).GetCode(), K_INVALID);
}

TEST(UrmaChipInflightTest, SkipsClientTransportNumaBindingWhenAffinityIsDisabled)
{
    auto &manager = UrmaManager::Instance();
    const bool oldNumaAffinity = manager.ubNumaAffinityEnabled_;
    const bool oldRegisterWholeArena = FLAGS_urma_register_whole_arena;
    manager.ubNumaAffinityEnabled_ = false;
    FLAGS_urma_register_whole_arena = true;

    EXPECT_TRUE(manager.BindClientTransportMemory(nullptr, 8192).IsOk());

    manager.ubNumaAffinityEnabled_ = oldNumaAffinity;
    FLAGS_urma_register_whole_arena = oldRegisterWholeArena;
}

TEST(UrmaChipInflightTest, NormalizesClientTransportPoolToPageAlignedArenaSizes)
{
    uint64_t effectiveSize = 0;
    EXPECT_TRUE(UrmaManager::NormalizeClientTransportPoolSize(8193, 2, 4096, effectiveSize).IsOk());
    EXPECT_EQ(effectiveSize, 16384u);

    EXPECT_TRUE(UrmaManager::NormalizeClientTransportPoolSize(16384, 2, 4096, effectiveSize).IsOk());
    EXPECT_EQ(effectiveSize, 16384u);

    constexpr uint64_t maxTransportMemSize = 2ULL * 1024ULL * 1024ULL * 1024ULL;
    EXPECT_EQ(UrmaManager::NormalizeClientTransportPoolSize(maxTransportMemSize, 3, 4096, effectiveSize).GetCode(),
              K_INVALID);
    EXPECT_EQ(UrmaManager::NormalizeClientTransportPoolSize(8192, 0, 4096, effectiveSize).GetCode(), K_INVALID);
    EXPECT_EQ(UrmaManager::NormalizeClientTransportPoolSize(8192, 2, 0, effectiveSize).GetCode(), K_INVALID);
}

TEST(UrmaChipInflightTest, SelectsSourceChipAccordingToRoundRobinType)
{
    auto &manager = UrmaManager::Instance();
    const uint32_t oldUbNumaRrType = FLAGS_ub_numa_rr_type;
    const uint32_t oldSrcChipPolicy = FLAGS_ub_numa_src_chip_policy;
    const uint32_t oldInflightDiffThreshold = manager.ubNumaInflightWrDiffThreshold_;
    FLAGS_ub_numa_src_chip_policy = static_cast<uint32_t>(UbNumaSrcChipPolicy::ROUND_ROBIN);
    manager.ubNumaInflightWrDiffThreshold_ = UB_NUMA_INFLIGHT_WR_DIFF_THRESHOLD;
    manager.srcChipInflightWrCounts_.at(1).value.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);

    FLAGS_ub_numa_rr_type = 0;
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID), 2);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, false, 2), 2);
    EXPECT_EQ(manager.affinitySrcChipIdSequence_.load(std::memory_order_relaxed), 0);

    FLAGS_ub_numa_rr_type = 1;
    // Two logical writes with two posts each: type 1 selects once on the first post and reuses that chip.
    auto firstWriteChip = manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID);
    EXPECT_EQ(firstWriteChip, 1);
    firstWriteChip = manager.GetAffinitySrcChipIdForPost(2, true, false, firstWriteChip);
    EXPECT_EQ(firstWriteChip, 1);
    auto secondWriteChip = manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID);
    EXPECT_EQ(secondWriteChip, 2);
    secondWriteChip = manager.GetAffinitySrcChipIdForPost(2, true, false, secondWriteChip);
    EXPECT_EQ(secondWriteChip, 2);
    EXPECT_EQ(manager.affinitySrcChipIdSequence_.load(std::memory_order_relaxed), 2);

    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    FLAGS_ub_numa_rr_type = 2;
    // Two logical writes with two posts each: type 2 selects independently for every post.
    auto chip = manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID);
    EXPECT_EQ(chip, 1);
    chip = manager.GetAffinitySrcChipIdForPost(2, true, false, chip);
    EXPECT_EQ(chip, 2);
    chip = manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID);
    EXPECT_EQ(chip, 1);
    chip = manager.GetAffinitySrcChipIdForPost(2, true, false, chip);
    EXPECT_EQ(chip, 2);
    EXPECT_EQ(manager.affinitySrcChipIdSequence_.load(std::memory_order_relaxed), 4);

    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(INVALID_CHIP_ID, false, true, INVALID_CHIP_ID), INVALID_CHIP_ID);
    EXPECT_EQ(manager.affinitySrcChipIdSequence_.load(std::memory_order_relaxed), 4);
    FLAGS_ub_numa_rr_type = oldUbNumaRrType;
    FLAGS_ub_numa_src_chip_policy = oldSrcChipPolicy;
    manager.ubNumaInflightWrDiffThreshold_ = oldInflightDiffThreshold;
}

TEST(UrmaChipInflightTest, OverridesRoundRobinOnlyWhenInflightDifferenceExceedsThreshold)
{
    auto &manager = UrmaManager::Instance();
    const uint32_t oldUbNumaRrType = FLAGS_ub_numa_rr_type;
    const uint32_t oldSrcChipPolicy = FLAGS_ub_numa_src_chip_policy;
    const uint32_t oldInflightDiffThreshold = manager.ubNumaInflightWrDiffThreshold_;
    FLAGS_ub_numa_rr_type = 2;
    FLAGS_ub_numa_src_chip_policy = static_cast<uint32_t>(UbNumaSrcChipPolicy::ROUND_ROBIN);
    manager.ubNumaInflightWrDiffThreshold_ = UB_NUMA_INFLIGHT_WR_DIFF_THRESHOLD;

    // The boundary is inclusive: a difference of exactly 15 keeps the round-robin candidate.
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(15, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID), 1);

    // Once the difference is 16, redirect away from the busy chip in either direction.
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(16, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID), 2);

    manager.affinitySrcChipIdSequence_.store(1, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(16, std::memory_order_relaxed);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID), 1);

    // Zero explicitly disables feedback and preserves the round-robin result despite imbalance.
    manager.ubNumaInflightWrDiffThreshold_ = 0;
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(100, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID), 1);

    manager.srcChipInflightWrCounts_.at(1).value.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
    FLAGS_ub_numa_rr_type = oldUbNumaRrType;
    FLAGS_ub_numa_src_chip_policy = oldSrcChipPolicy;
    manager.ubNumaInflightWrDiffThreshold_ = oldInflightDiffThreshold;
}

TEST(UrmaChipInflightTest, UsesAffinityOnlyWhenTheLogicalWriteFitsWithoutIncreasingRoundRobinDepth)
{
    auto &manager = UrmaManager::Instance();
    const uint32_t oldUbNumaRrType = FLAGS_ub_numa_rr_type;
    const uint32_t oldSrcChipPolicy = FLAGS_ub_numa_src_chip_policy;
    const uint32_t oldInflightDiffThreshold = manager.ubNumaInflightWrDiffThreshold_;
    FLAGS_ub_numa_rr_type = static_cast<uint32_t>(UbNumaRrType::PER_LOGICAL_WRITE);
    FLAGS_ub_numa_src_chip_policy = static_cast<uint32_t>(UbNumaSrcChipPolicy::ROUND_ROBIN_WITH_AFFINITY);
    manager.ubNumaInflightWrDiffThreshold_ = UB_NUMA_INFLIGHT_WR_DIFF_THRESHOLD;

    // Equal depths keep the RR candidate instead of herding concurrent requests onto the affinity chip.
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(4, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(4, std::memory_order_relaxed);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID, 2), 1);

    // Override a remote RR candidate only when the two-WR logical write fits without overtaking it.
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(6, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(3, std::memory_order_relaxed);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID, 2), 2);

    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(4, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(3, std::memory_order_relaxed);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID, 2), 1);

    // The hard threshold still overrides both RR and affinity decisions.
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(16, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(1, true, true, INVALID_CHIP_ID, 2), 2);

    // Zero disables all depth feedback, leaving pure RR selection.
    manager.ubNumaInflightWrDiffThreshold_ = 0;
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(100, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID, 2), 1);

    manager.srcChipInflightWrCounts_.at(1).value.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
    FLAGS_ub_numa_rr_type = oldUbNumaRrType;
    FLAGS_ub_numa_src_chip_policy = oldSrcChipPolicy;
    manager.ubNumaInflightWrDiffThreshold_ = oldInflightDiffThreshold;
}

TEST(UrmaChipInflightTest, ReusesLogicalWriteRoundRobinCandidateWhenGatherAffinityChanges)
{
    auto &manager = UrmaManager::Instance();
    const uint32_t oldSrcChipPolicy = FLAGS_ub_numa_src_chip_policy;
    const uint32_t oldInflightDiffThreshold = manager.ubNumaInflightWrDiffThreshold_;
    FLAGS_ub_numa_src_chip_policy = static_cast<uint32_t>(UbNumaSrcChipPolicy::ROUND_ROBIN_WITH_AFFINITY);
    manager.ubNumaInflightWrDiffThreshold_ = UB_NUMA_INFLIGHT_WR_DIFF_THRESHOLD;
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(1).value.store(6, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(3, std::memory_order_relaxed);

    const auto firstDecision = manager.BuildSrcChipSelectionDecision(1, 2);
    const auto secondDecision = manager.BuildSrcChipSelectionDecisionWithCandidate(2, 1, firstDecision.candidate);

    EXPECT_EQ(firstDecision.candidate, 1);
    EXPECT_EQ(firstDecision.selected, 1);
    EXPECT_EQ(secondDecision.candidate, 1);
    EXPECT_EQ(secondDecision.selected, 2);
    EXPECT_EQ(manager.affinitySrcChipIdSequence_.load(std::memory_order_relaxed), 1);

    manager.srcChipInflightWrCounts_.at(1).value.store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).value.store(0, std::memory_order_relaxed);
    FLAGS_ub_numa_src_chip_policy = oldSrcChipPolicy;
    manager.ubNumaInflightWrDiffThreshold_ = oldInflightDiffThreshold;
}

TEST(UrmaChipInflightTest, SelectsDominantGatherSourceChipByBytes)
{
    constexpr uint64_t smallWriteSize = 4;
    constexpr uint64_t largeWriteSize = 12;
    const std::vector<LocalSgeInfo> objInfos{
        LocalSgeInfo{ .segAddr = 0,
                      .segSize = 0,
                      .sgeAddr = 0,
                      .readOffset = 0,
                      .writeSize = smallWriteSize,
                      .metaDataSize = 0,
                      .srcChipId = 1 },
        LocalSgeInfo{ .segAddr = 0,
                      .segSize = 0,
                      .sgeAddr = 0,
                      .readOffset = 0,
                      .writeSize = largeWriteSize,
                      .metaDataSize = 0,
                      .srcChipId = 2 },
        LocalSgeInfo{ .segAddr = 0,
                      .segSize = 0,
                      .sgeAddr = 0,
                      .readOffset = 0,
                      .writeSize = largeWriteSize,
                      .metaDataSize = 0,
                      .srcChipId = INVALID_CHIP_ID },
    };

    EXPECT_EQ(UrmaManager::SelectDominantGatherSrcChipId(objInfos, 0, objInfos.size()), 2);
    EXPECT_EQ(UrmaManager::SelectDominantGatherSrcChipId(objInfos, 0, 1), 1);
}

TEST(UrmaChipInflightTest, LogsOnlyDepthOverridesOnTheSelectionHotPath)
{
    UrmaManager::SrcChipSelectionDecision decision{ 1, 2, 1, 15, 1 };
    decision.affinityOverride = true;
    EXPECT_FALSE(UrmaManager::ShouldLogSrcChipSelection(decision));

    decision.depthOverride = true;
    EXPECT_TRUE(UrmaManager::ShouldLogSrcChipSelection(decision));
}

TEST(UrmaChipInflightTest, NormalizesWorkerRoundRobinType)
{
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(0, "test-worker"), 0u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(1, "test-worker"), 1u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(2, "test-worker"), 2u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(3, "test-worker"), 1u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(UINT32_MAX, "test-worker"), 1u);
}

TEST(UrmaChipInflightTest, NormalizesWorkerSourceChipPolicy)
{
    EXPECT_EQ(UrmaManager::NormalizeUbNumaSrcChipPolicy(0, "test-worker"), 0u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaSrcChipPolicy(1, "test-worker"), 1u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaSrcChipPolicy(2, "test-worker"), 1u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaSrcChipPolicy(UINT32_MAX, "test-worker"), 1u);
}

}  // namespace
}  // namespace datasystem

#else
TEST(UrmaChipInflightTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma.";
}
#endif
