/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Tests for object cache recovery evidence.
 */
#include "datasystem/worker/object_cache/object_cache_recovery_evidence.h"

#include <gtest/gtest.h>

namespace datasystem {
namespace object_cache {
namespace {
TEST(ObjectCacheRecoveryEvidenceTest, MetadataRecoverySummaryMarksMetadataReadyOnlyWhenAllRequestedMetasRecover)
{
    MetaDataRecoveryManager::RecoverySummary partial;
    partial.requestedCount = 3;
    partial.recoveredCount = 2;
    partial.failedIds = { "object-a" };

    auto partialReport = BuildMetadataRecoveryEvidenceReport(partial);

    EXPECT_FALSE(partialReport.evidence.metadataReady);
    EXPECT_NE(partialReport.detail.find("metadata_recovered=2/3"), std::string::npos);
    EXPECT_NE(partialReport.detail.find("metadata_failed=1"), std::string::npos);

    MetaDataRecoveryManager::RecoverySummary complete;
    complete.requestedCount = 3;
    complete.recoveredCount = 3;

    auto completeReport = BuildMetadataRecoveryEvidenceReport(complete);

    EXPECT_TRUE(completeReport.evidence.metadataReady);
    EXPECT_NE(completeReport.detail.find("metadata_recovered=3/3"), std::string::npos);
}

TEST(ObjectCacheRecoveryEvidenceTest, EmptyMetadataRecoveryBatchIsReady)
{
    MetaDataRecoveryManager::RecoverySummary empty;

    auto report = BuildMetadataRecoveryEvidenceReport(empty);

    EXPECT_TRUE(report.evidence.metadataReady);
    EXPECT_NE(report.detail.find("metadata_recovered=0/0"), std::string::npos);
}

TEST(ObjectCacheRecoveryEvidenceTest, SlotRecoveryEvidenceRequiresEveryIncidentTerminalWithoutFailures)
{
    SlotRecoveryInfoPb complete;
    complete.set_total_slots(2);
    complete.set_completed_slots(2);

    SlotRecoveryInfoPb failed;
    failed.set_total_slots(2);
    failed.set_completed_slots(1);
    failed.set_failed_slots(1);

    auto failedReport = BuildSlotRecoveryEvidenceReport({ complete, failed });

    EXPECT_FALSE(failedReport.evidence.slotReady);
    EXPECT_NE(failedReport.detail.find("slot_incidents_ready=1/2"), std::string::npos);
    EXPECT_NE(failedReport.detail.find("slot_incidents_failed=1"), std::string::npos);

    auto completeReport = BuildSlotRecoveryEvidenceReport({ complete });

    EXPECT_TRUE(completeReport.evidence.slotReady);
    EXPECT_NE(completeReport.detail.find("slot_incidents_ready=1/1"), std::string::npos);
}

TEST(ObjectCacheRecoveryEvidenceTest, OwnershipEvidenceUsesExplicitMasterReconciliationResult)
{
    auto unresolved = BuildOwnershipRecoveryEvidenceReport(false, "master reconciliation pending");

    EXPECT_FALSE(unresolved.evidence.ownershipReady);
    EXPECT_NE(unresolved.detail.find("ownership_ready=false"), std::string::npos);
    EXPECT_NE(unresolved.detail.find("master reconciliation pending"), std::string::npos);

    auto ready = BuildOwnershipRecoveryEvidenceReport(true, "master ownership confirmed");

    EXPECT_TRUE(ready.evidence.ownershipReady);
    EXPECT_NE(ready.detail.find("ownership_ready=true"), std::string::npos);
}

TEST(ObjectCacheRecoveryEvidenceTest, ObjectCacheRecoveryAggregateRequiresMetadataAndSlotReadiness)
{
    MetaDataRecoveryManager::RecoverySummary metadataSummary;
    metadataSummary.requestedCount = 1;
    metadataSummary.recoveredCount = 1;
    auto metadataReport = BuildMetadataRecoveryEvidenceReport(metadataSummary);

    SlotRecoveryInfoPb completeSlot;
    completeSlot.set_total_slots(1);
    completeSlot.set_completed_slots(1);
    auto slotReport = BuildSlotRecoveryEvidenceReport({ completeSlot });

    auto ownershipReport = BuildOwnershipRecoveryEvidenceReport(true, "master ownership confirmed");
    auto aggregate = BuildObjectCacheRecoveryEvidenceReport(metadataReport, slotReport, ownershipReport, true);

    EXPECT_TRUE(aggregate.evidence.metadataReady);
    EXPECT_TRUE(aggregate.evidence.slotReady);
    EXPECT_TRUE(aggregate.evidence.ownershipReady);
    EXPECT_TRUE(aggregate.evidence.resourceReady);
    EXPECT_NE(aggregate.detail.find("master ownership confirmed"), std::string::npos);
}

TEST(ObjectCacheRecoveryEvidenceTest, ObjectCacheRecoveryAggregateRequiresAllocatorHeadroom)
{
    MetaDataRecoveryManager::RecoverySummary metadataSummary;
    metadataSummary.requestedCount = 1;
    metadataSummary.recoveredCount = 1;
    auto metadataReport = BuildMetadataRecoveryEvidenceReport(metadataSummary);
    SlotRecoveryInfoPb completeSlot;
    completeSlot.set_total_slots(1);
    completeSlot.set_completed_slots(1);
    auto slotReport = BuildSlotRecoveryEvidenceReport({ completeSlot });

    auto ownershipReport = BuildOwnershipRecoveryEvidenceReport(true, "master ownership confirmed");
    auto aggregate = BuildObjectCacheRecoveryEvidenceReport(metadataReport, slotReport, ownershipReport, false);

    EXPECT_TRUE(aggregate.evidence.ownershipReady);
    EXPECT_FALSE(aggregate.evidence.resourceReady);
}

TEST(ObjectCacheRecoveryEvidenceTest, ObjectCacheRecoveryAggregateBlocksOwnershipWhenMasterEvidenceIsMissing)
{
    MetaDataRecoveryManager::RecoverySummary metadataSummary;
    metadataSummary.requestedCount = 1;
    metadataSummary.recoveredCount = 1;
    auto metadataReport = BuildMetadataRecoveryEvidenceReport(metadataSummary);

    SlotRecoveryInfoPb completeSlot;
    completeSlot.set_total_slots(1);
    completeSlot.set_completed_slots(1);
    auto slotReport = BuildSlotRecoveryEvidenceReport({ completeSlot });
    auto ownershipReport = BuildOwnershipRecoveryEvidenceReport(false, "master primary not confirmed");

    auto aggregate = BuildObjectCacheRecoveryEvidenceReport(metadataReport, slotReport, ownershipReport, true);

    EXPECT_TRUE(aggregate.evidence.metadataReady);
    EXPECT_TRUE(aggregate.evidence.slotReady);
    EXPECT_FALSE(aggregate.evidence.ownershipReady);
    EXPECT_NE(aggregate.detail.find("ownership_ready=false"), std::string::npos);
}
}  // namespace
}  // namespace object_cache
}  // namespace datasystem
