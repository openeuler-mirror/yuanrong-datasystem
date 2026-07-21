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
 * Description: Worker recovery evidence tracker tests.
 */
#include "datasystem/worker/runtime/worker_recovery_evidence_tracker.h"

#include "gtest/gtest.h"

namespace datasystem::worker {
namespace {
TEST(WorkerRecoveryEvidenceTrackerTest, OldGenerationEvidenceIsRejected)
{
    WorkerRecoveryEvidenceTracker tracker;
    auto oldGeneration = tracker.BeginRecovery("first");
    WorkerRecoveryEvidenceBuilder builder;
    ASSERT_TRUE(tracker.UpdateEvidence(oldGeneration, builder.MarkMembershipReady()
                                                          .MarkTopologyReady()
                                                          .MarkMetadataReady()
                                                          .MarkSlotReady()
                                                          .MarkOwnershipReady()
                                                          .MarkResourceReady()
                                                          .BuildReport()));
    ASSERT_TRUE(tracker.IsComplete(oldGeneration));

    auto newGeneration = tracker.BeginRecovery("second");
    EXPECT_FALSE(tracker.IsComplete(newGeneration));
    EXPECT_FALSE(tracker.UpdateEvidence(oldGeneration, builder.BuildReport("stale")));
    EXPECT_FALSE(tracker.GetEvidence(oldGeneration).has_value());
    EXPECT_FALSE(tracker.IsComplete(newGeneration));
}

TEST(WorkerRecoveryEvidenceTrackerTest, EmptyEvidenceDoesNotCompleteNewRecovery)
{
    WorkerRecoveryEvidenceTracker tracker;
    auto generation = tracker.BeginRecovery("isolation");
    WorkerRecoveryEvidenceBuilder builder;
    ASSERT_TRUE(tracker.UpdateEvidence(generation, builder.MarkMembershipReady().BuildReport("partial")));

    EXPECT_FALSE(tracker.IsComplete(generation));
    auto evidence = tracker.GetEvidence(generation);
    if (!evidence.has_value()) {
        FAIL() << "partial evidence should be available for current recovery generation";
    }
    EXPECT_EQ(evidence.value().generation, generation);
    EXPECT_FALSE(evidence.value().report.evidence.metadataReady);
}
}  // namespace
}  // namespace datasystem::worker
