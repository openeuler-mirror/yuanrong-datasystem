/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Tests for object-cache recovery state aggregation.
 */
#include "datasystem/worker/object_cache/object_cache_recovery_state.h"

#include <gtest/gtest.h>

namespace datasystem {
namespace object_cache {
namespace {

worker::WorkerRecoveryEvidenceReport CompleteObjectCacheReport()
{
    worker::WorkerRecoveryEvidenceBuilder builder;
    return builder.MarkMetadataReady().MarkSlotReady().MarkOwnershipReady().MarkResourceReady().BuildReport("complete");
}

TEST(ObjectCacheRecoveryStateTest, MetadataSummaryUpdatesLatestEvidence)
{
    ObjectCacheRecoveryState state;
    EXPECT_TRUE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);

    MetaDataRecoveryManager::RecoverySummary partial;
    partial.requestedCount = 2;
    partial.recoveredCount = 1;
    partial.failedIds = { "object-a" };
    state.SetMetadataRecoverySummary(partial);
    EXPECT_FALSE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);

    MetaDataRecoveryManager::RecoverySummary complete;
    complete.requestedCount = 2;
    complete.recoveredCount = 2;
    state.SetMetadataRecoverySummary(complete);
    EXPECT_TRUE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);
}

TEST(ObjectCacheRecoveryStateTest, ResourceRecoveryUsesGenerationToRejectStaleCommit)
{
    ObjectCacheRecoveryState state;
    const auto staleGeneration = state.MarkResourceRecoveryRequired(memory::CacheType::MEMORY);
    const auto currentGeneration = state.MarkResourceRecoveryRequired(memory::CacheType::DISK);

    EXPECT_FALSE(state.PublishResourceRecoveryIfCurrent(staleGeneration, [] { return true; }));
    auto snapshot = state.GetResourceRecoverySnapshot();
    EXPECT_TRUE(snapshot.memoryRequired);
    EXPECT_TRUE(snapshot.diskRequired);
    EXPECT_EQ(snapshot.generation, currentGeneration);

    EXPECT_TRUE(state.PublishResourceRecoveryIfCurrent(currentGeneration, [] { return true; }));
    snapshot = state.GetResourceRecoverySnapshot();
    EXPECT_FALSE(snapshot.memoryRequired);
    EXPECT_FALSE(snapshot.diskRequired);
}

TEST(ObjectCacheRecoveryStateTest, EvidenceGenerationInvalidatesOldReport)
{
    ObjectCacheRecoveryState state;
    const auto oldGeneration = state.BeginRecoveryEvidenceGeneration("old");
    EXPECT_TRUE(state.TrackEvidenceForGeneration(oldGeneration, CompleteObjectCacheReport()).evidence.ownershipReady);

    const auto newGeneration = state.BeginRecoveryEvidenceGeneration("new");
    EXPECT_FALSE(state.TrackEvidenceForGeneration(oldGeneration, CompleteObjectCacheReport()).evidence.ownershipReady);
    EXPECT_TRUE(state.TrackEvidenceForGeneration(newGeneration, CompleteObjectCacheReport()).evidence.ownershipReady);
}

}  // namespace
}  // namespace object_cache
}  // namespace datasystem
